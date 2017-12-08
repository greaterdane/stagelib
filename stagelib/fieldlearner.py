import re
from string import punctuation
from time import sleep
from functools import partial
from collections import defaultdict
import numpy as np

re_SEMI = re.compile(r'(?:\s+)?;(?:\s+)?')
re_DUPECOL = re.compile(r'\.\d+$')
re_NONFIELD = re.compile('^(?:\s+)?(?:(?:\$)?\d+(?:[-\/\.\s,]+|$)|[%s]|[\|,\t]+(?:\s+)?)' % punctuation)

sample_template = (
"""
'{field}'
{border}
{sample}
""").format

skip_text = "If you would like to leave '{field}' as is, type the return key."
prompt_template = (
"""
Current File: {path}
{border}
{sample_display}
Enter a field name from table '{table}' for '{field}'.
Enter '-fields' to show fields in data.
Enter '-choices' to show choices for column renaming.
{skip_text}
""").format

fields_template = (
"""

{message}
{border}
{fields}
"""
).format

modify_template = (
"""
Please enter each field and its respective modification (FIELD : NEWNAME).
Entries must be separated by a semi-colon (;), e.g. "FIELD1 : NEWNAME1; FIELD2 : NEWNAME2".
And please remember to relax, all whitespace will be accounted for.
=============================================================================================

"""
)

arrowformat = "{} --> {}".format
error_message = """
ERROR: table '{table}' does not contain field '{choice}'.""".format

choice_continue = """Enter 1 to continue with current choice, otherwise please try again.\n{skip_text}\n""".format

BORDER = "=" * 13

def show(func):
    def inner(*args):
        message, fields = func(*args)
        input = raw_input(
            fields_template(**{
                'message' : message,
                'fields' : fields,
                'border' : BORDER
                    }))
        return input
    return inner

@show
def currentfields(fieldsmap, fields):
    return ("CURRENT FIELDS\nIf you would like to change the value of a renamed column, enter '-modify'.\nOtherwise, press any key to continue.",
            '\n'.join(arrowformat(i, fieldsmap.get(i, 'N/A')) for i in fields))
@show
def choices(fields):
    return "FIELD CHOICES\n", '\n'.join(fields)

def clean(choice, *args):
    if choice.startswith('-'):
        choice = choice.replace(' ', '')
    return choice.strip(*args)

def getsample(df, field):
    return '\n'.join(
        df.loc[df[field].notnull(), field]\
            .head(20).fillna('N/A')\
            .astype(str).tolist()
                )

def get_sample_display(field, sample):
    return sample_template(field = field, sample = sample, border = BORDER)

def modify():
    d = {}
    changes = clean(  raw_input(modify_template), ';'  )
    if changes:
        for item in re_SEMI.split(changes):
            k, v = map(clean, item.split(':'))
            d.update(  eval("{'%s' : '%s'}" % (k, v))  )
    return d

def choose(df, field, fieldsmap, fields, **kwds):
    sample = getsample(df, field)
    if not sample:
        return {}

    table = kwds.get('table') or 'N/A'
    __ = dict(sample_display = get_sample_display(field, sample),
              path = kwds.get('path') or 'DataFrame',
              table = table,
              field = field,
              skip_text = skip_text.format(field = field),
              border = BORDER)

    while True:
        choice = clean(raw_input(prompt_template(**__)))
        if choice == '-fields':
            modifychoice = currentfields(fieldsmap, df.columns.astype(str))
            if modifychoice == clean('-modify'):
                fieldsmap.update(  modify()  )
        elif choice == '-choices':
            if fields:
                choices(fields)
            else:
                print "Invalid option.  There are currently no restrictions on field choices.\n"
                sleep(0.1)
        elif (fields != [] and (choice and choice not in fields)):
            print; print error_message(table = table,
                                       choice = choice)

            _choice = raw_input( choice_continue(skip_text = skip_text.format(field = field)) )
            if _choice == '1' or _choice in fields or not _choice:
                choice = _choice; break
            else:
                print "Invalid choice.  Please try again.\n"
        else:
            if not choice:
                choice = field
            break

    print arrowformat(field, choice); print
    return {field : choice}

def get_dupefields(fields):
    d = defaultdict(int)
    for field in fields:
        d[field] += 1
    return [
        k for k, v in d.items() if v > 1
            ]

def dedupefields(fields, dupes = []):
    if not dupes:
        dupes = get_dupefields(fields)

    for dupefield in dupes:
        idx = [i for i, field in enumerate(fields) if field == dupefield][1:]
        for _i, i in enumerate(idx, 1):
            fields[i] = "%s.%s" % (dupefield, _i)
    return fields

def learnfields(df, fieldsmap, fields = [], table = '', path = '', endcheck = False):
    if set(fields).issuperset(df.columns):
        return fieldsmap
    
    dfields = df.columns.astype(str)
    for field in dfields:
        if field in fieldsmap:
            continue
        fieldsmap.update(  choose(df, field, fieldsmap, fields)  )

    if endcheck and get_dupefields([fieldsmap[i] for i in dfields]):
        fieldsmap.update(  modify()  )
    return fieldsmap
    
is_nonfield = partial(re.search, re_NONFIELD)
def nonfields(row, length, thresh = 0.4):
    pctblank = row.count('')/length
    is_data = any(  map(is_nonfield, row)  ) #cells containing 'non field' data, e.g. numbers, other misc numeric data, dates, etc.
    return pctblank >= thresh or is_data or not row[0 : int(length * .5)] #False if % of blanks >= thresh, the row contains data, or the first half is empty.

def locatefields(rows, **kwds):
    lens = {i : len(l)  for i,l in enumerate(rows)}
    ml = np.bincount(lens.values()).argmax()
    for ix, v in lens.items():
        if v >= (ml):
            break

    for i, row in enumerate(rows[ix:], ix):
        row = map(lambda x: x.strip().replace('\n', ' '), row)
        if nonfields(row, ml, **kwds):
            continue
        return i + 1, row
    else:
        return ix, Tabular.createheader(ml)
