from airtable import Airtable
import sys, csv, re, argparse, os, json

parser = argparse.ArgumentParser(description='Import CSV data from stdin to an existing AirTable table')
parser.add_argument('--api-key', dest='apiKey', required=False, help='API key for AirTable, defaults to environment variable AIRTABLE_API_KEY')
parser.add_argument('--base-key', dest='baseKey', required=False, help='Base key for AirTable, defaults to environment variable AIRTABLE_BASE_KEY')
parser.add_argument('-t', '--table', dest='tableName', required=True, help='Name of table in AirTable')
parser.add_argument('-u', '--update', dest='updateOnConflict', required=False, action='store_true')
parser.add_argument('-r', '--replace', dest='replaceOnConflict', required=False, action='store_true')
parser.add_argument('-a', '--attachment-fields', dest='attachmentFields', default=[], required=False, nargs='+', help='List any fields that are attachment type')
parser.add_argument('--id-field', dest='idField', required=False, help='name of primary field, only needed with --update or --replace')
parser.add_argument('--json', dest='isJson', required=False, action='store_true', help='Input is json (default is CSV)')
args = parser.parse_args()

apiKey = args.apiKey or os.environ.get('AIRTABLE_API_KEY')
baseKey = args.baseKey or os.environ.get('AIRTABLE_BASE_KEY')
tableName = args.tableName
conflictPolicy = (args.updateOnConflict and 'update') or (args.replaceOnConflict and 'replace') or 'insert'
attachmentFields = args.attachmentFields
idField = args.idField


if ((args.updateOnConflict or args.replaceOnConflict) and idField is None):
    sys.stderr.write('You must specify a primary field when using --update or --replace')
    sys.exit(1)
if (apiKey is None):
    sys.stderr.write('You must specify an api key in environment (AIRTABLE_API_KEY) or --api-key\n')
    sys.exit(1)
if (apiKey is None):
    sys.stderr.write('You must specify a base key in environment (AIRTABLE_BASE_KEY) or --base-key\n')
    sys.exit(1)

if (args.updateOnConflict and args.replaceOnConflict):
    sys.stderr.write('Cannot specify both --update and --replace\n')
    sys.exit(1)

table = Airtable(baseKey, tableName, apiKey)
reader = json.load(sys.stdin) if args.isJson else csv.DictReader(sys.stdin, delimiter=',', quotechar='"')

count = 0
for record in reader:

    # reformat attachment fields
    for f in attachmentFields:
        if (f in record):
            record[f] = [{'url': record[f]}]

    if (conflictPolicy == 'insert'):
        table.insert(record)
    elif (conflictPolicy == 'replace'):
        if (table.replace_by_field(idField, record[idField], record) == {}):
            table.insert(record)
    elif (conflictPolicy == 'update'):
        if (table.update_by_field(idField, record[idField], record) == {}):
            table.insert(record)

    count = count + 1
    sys.stdout.write(str(count) if count % 10 == 0 else '.') and sys.stdout.flush()

sys.stdout.write('Done\n')
sys.stdout.write('Processed ' + str(count) + ' records\n')

# Possible improvements
#   * auto-detect attachment fields instead of needing to specify as argument
#   * ability to map from column labels in CSV to airtable field names or (better) id
#   * only supports single attachment
#   * on update, only pass fields with changing value