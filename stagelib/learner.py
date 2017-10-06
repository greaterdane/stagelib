template = (
"""
Current File: {path}
{border}

Field selection.
{border}
{fields}

'{field}'
{border}
{sample}

Enter a field name from '{table}' for '{field}'.
Press return key when finished.
""")

error_prompt = """ERROR: '{table}' does not contain field '{choice}'."""
choice_continue = """If you would like to accept anyway, enter 1, otherwise please try again.\n"""
update_message = "'{field}' was renamed '{choice}'."

_char = '='
border = _char * 13

def learn_fields(df, fields_map, fields = [], table = '', path = '', strict = True):
    field_updates = {}
    if all(i in fields for i in df.columns):
        return fields_map

    start = 0
    end = 0
    for field in df.columns:
        if str(field) in fields_map:
            continue
        _ = '\n'.join(df[field].head(20)\
            .fillna('N/A')\
            .astype(str).tolist())

        params = dict(sample = _,
                    path = path,
                    field = field,
                    border = border,
                    table = table,
                    fields =  '\n'.join(map(str, df.columns[start:end])))

        choice = raw_input(template.format(**params)); print
        while True:
            if not choice:
                choice = field
                break
            if strict and (fields and choice not in fields):
                print error_prompt.format(table = table, choice = choice)
                choice2 = raw_input(choice_continue)
                print
                print
                if choice2 == '1':
                    break
                choice = choice2

            start += 1
            end += 1
            break
        fields_map.update({field : choice})
    return fields_map