import re
import logging
from functools import wraps
import numpy as np
import pandas as pd
from fuzzywuzzy import fuzz

from stagelib.generic import fuzzyprep, mergedicts
from stagelib.files import df2excel, newfolder, joinpath
import stagelib.dataframe
from stagelib.dataframe import quickmapper

logging.basicConfig(format='%(asctime)s|%(message)s', datefmt='%Y-%m-%d %I:%M:%S %p')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def results_to_csv(outfile, df):
    df.to_csv(outfile, index = False, encoding = 'utf-8')

def disected_match(df, length = 1):
    return df.apply(lambda x: disect_string(x['match_x'],
        length = length) == disect_string(x['match_y'],
        length = length), axis = 1)

def disect_string(x, length = 1):
    return fuzzyprep(''.join(x.split()[0:length]))

@quickmapper
def categorize_name(x, N = 3):
    """Find the first N elements of a name,
        starting after and including the first occurence
        of a CAPITAL LETTER or DIGIT, to group by first N elements.
    Returns a lower cased string.

    Parameters:
    ----------
    x : Item/name to categorize. str
    [n] : The first number of elements to use in categorizing x (2 is default). int

    Example:
    --------
    categorize_name("d/b/a Company name") -- > "co"
    """
    i = 0
    idx = map(lambda y: (y.isalpha() and y.isupper()) or y.isdigit(), x)
    if True in idx:
        i = idx.index(True)

    return fuzzyprep(x[i:])[:N]

def fuzzytable(series):
    return pd.DataFrame({'group' : categorize_name(series),
                         'fuzzy' : series.fuzzyprep()}, index = series.index).dropna()

def get_exact_matches(x_df, y_df):
    """Create a dictionary of fuzzy column values (keys) and index (value)
        from y_df (matches FROM) and map it to the x_df (matches FOR) fuzzy column.
        Returns a series which represents the index locations of matches FROM y_df.

    Parameters:
    ----------
    x_df : The data you are trying to get matches FOR. pd.DataFrame
    y_df : The data you are trying to pull matches FROM. pd.DataFrame
    """
    return x_df.fuzzy\
        .map(y_df.reset_index()\
        .get_mapper('fuzzy', 'index'))\
        .reindex(x_df.index)

def create_groups(x_df, y_df):
    """Create pd.DataFrame.groupby objects where there is
        overlap in the 'letter_group' column between both datasets.
    """
    __ = {1 : x_df, 2 : y_df}
    for i in range(1,3):
        yield 'group%s' % i, __[i]\
            .loc[__[i]\
            .group\
            .isin(__.get(i + 1, x_df)\
            .group)]\
            .groupby('group')

@quickmapper
def get_fuzzy_ranking(x, y):
    return {'ratio_total' : fuzz.ratio(x, y),
            'ratio_partial' : fuzz.partial_ratio(x, y)}

def is_a_partial_match(rankings, threshold = 70):
    return ((rankings['ratio_partial'] >= 97) & (rankings['ratio_total'] < threshold - 20))

def is_a_match(rankings, threshold = 87):
    return (rankings['ratio_total'] >= threshold) & (rankings['ratio_partial'] >= 95)

def is_a_possible_match(rankings, threshold = 70):
    return (is_a_partial_match(rankings) |
        ((rankings['ratio_total'] < threshold) & (rankings['ratio_partial'] >= 80)) |\
        ((rankings['ratio_total'] >= threshold - 20) & (rankings['ratio_partial'] < 97))
            )

def not_a_match(rankings):
    return ~(is_a_match(rankings)) & ~(is_a_possible_match(rankings))

#matches with score of 180 +, if first word matches, safe to say a match
def most_likely(df):
    return (df.score >= 180) & (disected_match(df))

#matches with a score less than 180 and greater than 163, if first two words match and len(match_x) >= len(match_y), safe to say a match
def likely(df):
    return ((df.score > 163) & (df.score < 180)) & disected_match(df, length = 2)

#the remainder of possible matches
def least_likely(df):
    return ~(most_likely(df)) & ~(likely(df))

def categorize_matches(rankings):
    __ = {
        'MATCH' : is_a_match(rankings),
        'POSSIBLE_MATCH' : is_a_possible_match(rankings),
        'NON_MATCH' : not_a_match(rankings)
            }

    for category, mask in __.items():
        logger.info("% results: %s" % (category, len(rankings.loc[mask])))
        rankings.loc[mask, 'match_category'] = category
    return rankings.assign(
        score = rankings['ratio_total'] + rankings['ratio_partial'])

def get_rankings(x_df, y_df, match_col, threshold = 88, **kwds):

    x, y = [fuzzytable(v[match_col]).assign(**{"match_%s" % k : v[match_col]})
            for k, v in (['x', x_df], ['y', y_df],)]

    #Level 1 matches: Pre-processed exact.
    match_index = get_exact_matches(x, y)
    mdict = match_index\
        .loc[match_index.notnull()]\
        .to_numeric(integer = True)\
        .to_dict()

    x_ix, y_ix = mdict.keys(), mdict.values()
    rankings = pd.DataFrame({
        'group' : 'EXACT_MATCH',
        'ratio_total' : 100,
        'ratio_partial' : 100,
        'match_x' : x.ix[x_ix, 'match_x'].values,
        'match_y' : y.ix[y_ix, 'match_y'].values,
        'x_index' : x_ix,
        'y_index' : y_ix},
            index = x_ix)

    #Level 2 matches: By category groups.
    groupdict = dict(create_groups(x.drop(x_ix), y))
    for name, df in groupdict['group1']:
        data = []
        group2 = groupdict['group2'].get_group(name)
        logger.info("%s matches queued for strings beginning with '%s'" % (len(df) * len(group2), name))
        for i, row in df.iterrows():
            logger.info("Finding matches for '%s'" % row.match_x)
            data.extend([
                mergedicts(d, dict(row),
                    x_index = i,
                    y_index = group2.index[i2],
                    match_y = group2.iloc[i2]['match_y']
                        ) for i2, d in enumerate(
                            list(get_fuzzy_ranking(group2.fuzzy, row.fuzzy)))
                                ])
        rankings = rankings\
        .append(pd.DataFrame(data,
            columns = rankings.columns))

    logger.info("%s matches performed" % len(rankings))
    return categorize_matches(rankings)

def fuzzymatch(x_df, y_df, match_col, outfile_prefix = '', intern_folder = 'check', **kwds):
    match_groups = get_rankings(x_df,
        y_df, match_col, **kwds).groupby('match_category')

    results_to_csv("%s_Non-matches.csv" % outfile_prefix,
        match_groups.get_group('NON_MATCH'))
    try:
        matches = match_groups.get_group('MATCH')
        results_to_csv("%s_Matches.csv" % outfile_prefix, matches)
    except KeyError:
        logger.error("No certain matches found.", exec_info = True)
        matches = pd.DataFrame()

    internfile = joinpath(newfolder(intern_folder),
        "%s_Possible Matches.xlsx" % outfile_prefix)

    pm = match_groups.get_group('POSSIBLE_MATCH')
    possible_match_groups = dict(most_likely = pm.loc[most_likely],
        likely = pm.loc[likely],
        least_likely = pm.loc[least_likely])

    for k, v in possible_match_groups.items():
        logger.info("%s '%s' possible matches found." % (len(v), k.replace('_', ' ')))

    logger.info("Writing possible matches to '%s'" % internfile)
    df2excel(internfile, **{k : v.sort_values(by = ['match_y','score'], ascending = False)
                           for k, v in possible_match_groups.items()})
    return matches

def concat_matches(matches, x_df, y_df, x_suffix = "x", y_suffix = "y"):
    return matches.filter(regex = '_index')\
        .merge(x_df,
            left_on = 'x_index',
            right_index = True)\
        .merge(y_df,
            left_on = 'y_index',
            right_index = True,
            suffixes = ("_%s" % x_suffix, "_%s" % y_suffix))

def matchsetup(match_col):
    """Closure used to create a decorator
        which will pass the given parameters
        to and execute 'fuzzymatch'.

       **The decorated inner function is
        REQUIRED to produce x_df and y_df.

        Returns a side by side view
        of the resulting matches
        and data (pd.DataFrame).

        Parameters
        ----------
        match_col : Common field name to match on.
    """
    def decorator(func):
        @wraps(func)
        def inner(x_df, y_df, *args, **kwds):
            return fuzzymatch(x_df, y_df, match_col, **kwds)
        return inner
    return decorator
