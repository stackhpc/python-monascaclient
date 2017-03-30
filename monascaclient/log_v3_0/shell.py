# Copyright 2017 StackHPC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import time

from monascaclient.common import utils
import monascaclient.exc as exc

allowed_log_sort_by = ['timestamp']


@utils.arg('--dimensions', metavar='<KEY1=VALUE1,KEY2=VALUE2...>',
           help='key value pair used to specify a metric dimension. '
           'This can be specified multiple times, or once with parameters '
           'separated by a comma. '
           'Dimensions need quoting when they contain special chars [&,(,),{,},>,<] '
           'that confuse the CLI parser.',
           action='append')
@utils.arg('--starttime', metavar='<UTC_START_TIME>',
           help='measurements >= UTC time. format: 2014-01-01T00:00:00Z. OR format: -120 (previous 120 minutes).')
@utils.arg('--endtime', metavar='<UTC_END_TIME>',
           help='measurements <= UTC time. format: 2014-01-01T00:00:00Z.')
@utils.arg('--sort-by', metavar='<SORT BY FIELDS>',
           help='Fields to sort by as a comma separated list. Valid values are '
                'timestamp. '
                'Fields may be followed by "asc" or "desc", ex "timestamp desc", '
                'to set the direction of sorting.')
@utils.arg('--offset', metavar='<OFFSET LOCATION>',
           help='The offset used to paginate the return data.')
@utils.arg('--limit', metavar='<RETURN LIMIT>',
           help='Amount of logs to be returned up to the API maximum limit.')
def do_log_list(mc, mlc, args):
    '''List log messages.'''

    if mlc is None:
        raise exc.CommandError(
            'Command log-list requires log api endpoint '
            '(MONASCA_LOG_API_URL or --monasca-log-api-url)')

    fields = {}
    if args.dimensions:
        fields['dimensions'] = utils.format_parameters(args.dimensions)
    if args.starttime:
        _translate_starttime(args)
        fields['start_time'] = args.starttime
    if args.endtime:
        fields['end_time'] = args.endtime
    if args.limit:
        fields['limit'] = args.limit
    if args.offset:
        fields['offset'] = args.offset
    if args.sort_by:
        sort_by = args.sort_by.split(',')
        for field in sort_by:
            field_values = field.lower().split()
            if len(field_values) > 2:
                print("Invalid sort_by value {}".format(field))
            if field_values[0] not in allowed_log_sort_by:
                print("Sort-by field name {} is not in [{}]".format(field_values[0],
                                                                    allowed_log_sort_by))
                return
            if len(field_values) > 1 and field_values[1] not in ['asc', 'desc']:
                print("Invalid value {}, must be asc or desc".format(field_values[1]))
        fields['sort_by'] = args.sort_by

    try:
        logs = mlc.logs.list(**fields)
    except exc.HTTPException as he:
        raise exc.CommandError(
            'HTTPException code=%s message=%s' %
            (he.code, he.message))
    else:
        output_log_list(args, logs)


def output_log_list(args, logs):
    if args.json:
        print(utils.json_formatter(logs))
        return
    cols = ['timestamp', 'dimensions', 'message']
    formatters = {
        'timestamp': lambda x: x['timestamp'],
        'dimensions': lambda x: utils.format_dict(x['dimensions']),
        'message': lambda x: x['message'],
    }
    utils.print_list(logs, cols, formatters=formatters)


def _translate_starttime(args):
    if args.starttime[0] == '-':
        deltaT = time.time() + (int(args.starttime) * 60)
        utc = str(datetime.datetime.utcfromtimestamp(deltaT))
        utc = utc.replace(" ", "T")[:-7] + 'Z'
        args.starttime = utc


@utils.arg('dimensions',
           metavar='<dimension[=value],dimension[=value],...>',
           help='key value pair used to specify a metric dimension. '
           'This can be specified multiple times, or once with parameters '
           'separated by a comma. '
           'Dimensions need quoting when they contain special chars '
           ' [&,(,),{,},>,<] that confuse the CLI parser.',
           action='append',
           nargs='?')
@utils.arg('--limit', metavar='<RETURN LIMIT>', type=int, default=10,
           help='Amount of logs to be returned up to the API maximum limit.')
def do_log_tail(mc, mlc, args):
    '''Tail the most recent log messages.'''

    if args.json:
        raise exc.CommandError(
            'Command log-tail does not support --json (try log-list)')

    if mlc is None:
        raise exc.CommandError(
            'Command log-tail requires log api endpoint '
            '(MONASCA_LOG_API_URL or --monasca-log-api-url)')

    fields = {}
    if args.dimensions and args.dimensions != [None]:
        fields['dimensions'] = utils.format_parameters(args.dimensions)
    if args.limit:
        fields['limit'] = args.limit

    fields['sort_by'] = ['timestamp desc']

    try:
        logs = mlc.logs.list(**fields)
    except exc.HTTPException as he:
        raise exc.CommandError(
            'HTTPException code=%s message=%s' %
            (he.code, he.message))
    else:
        output_log_tail(args, logs)


def output_log_tail(args, logs):
    known_dimensions = [v.split('=')[0] for v in args.dimensions if v]
    line_format = u'{timestamp} [{dimensions}]: {message}'

    for l in reversed(logs):
        # Print dimensions sorted by name for consistency between lines.
        dimensions = sorted(l['dimensions'].items())
        # Remove dimensions we are matching - the value is always the same.
        dimensions = [d for d in dimensions if d[0] not in known_dimensions]
        fields = {'timestamp': l['timestamp'],
                  'dimensions': ' '.join(v for k, v in dimensions),
                  'message': l['message']}
        print(line_format.format(**fields))
