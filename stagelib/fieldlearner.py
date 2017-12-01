import re
from time import sleep
from collections import defaultdict

re_SEMI = re.compile(r'(?:\s+)?;(?:\s+)?')

sample_template = (
"""
'{field}'
{border}
{sample}
""").format

prompt_template = (
"""
Current File: {path}
{border}
{sample_display}
Enter a field name from '{table}' for '{field}'.
Enter '-fields' to show fields in data.
Enter '-choices' to show choices for column renaming.
""").format

finished_message = "\nPress return key when finished.\n"
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
error_prompt = """ERROR: '{table}' does not contain field '{field}'.""".format
choice_continue = """Enter 1 to continue with current choice, otherwise please try again."""
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

    table = kwds.get('table') or 'Not Specified'
    __ = dict(sample_display = get_sample_display(field, sample),
              path = kwds.get('path') or 'DataFrame',
              table = table,
              field = field,
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
                sleep(0.2)
        elif choice not in fields:
            print error_prompt(table = table, field = choice)
            _choice = raw_input( choice_continue )
            if _choice == '1' or _choice in fields or not _choice:
                choice = _choice; break
            else:
                print "Invalid choice.  Please try again.\n"
        elif not choice:
            choice = field; break
        else:
            break

    return {field : choice}

def deduplicate(fieldsmap):
    dictlist = defaultdict(list)
    for k, v in fieldsmap.items():
        dictlist[v].append(k)
    
    dupenames = [k for k, v in dictlist.items() if len(v) > 1]
    for name in dupenames:
        for i, val in enumerate(dictlist[name][1:], 1):
            fieldsmap[val] = "%s.%s" % (name, i)
    return fieldsmap

def learnfields(df, fieldsmap, fields = [], table = '', path = '', strict = True, endcheck = False):
    if set(fields).issuperset(df.columns):
        return fieldsmap
    
    df_fields = df.columns.astype(str)
    for field in df_fields:
        if field in fieldsmap:
            continue
        fieldsmap.update(  choose(df, field, fieldsmap, fields)  )

    fieldsmap = deduplicate(fieldsmap)
    if endcheck:
        fieldsmap.update(  modify()  )
    return fieldsmap
