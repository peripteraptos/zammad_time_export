from zammad_py import ZammadAPI
import pandas as pd
import argparse
from datetime import datetime


ap = argparse.ArgumentParser()
ap.add_argument("--url", type=str, required=True, help="Zammad API url (like https://tickets.dummy.com/api/v1/)")
ap.add_argument("--username", type=str, required=True, help="Username")
ap.add_argument("--password", type=str, required=True, help="Password")
ap.add_argument("--year", type=int, required=False,
                default=None, help="Year")
ap.add_argument("--month", type=int, required=False,
                default=None, help="Month")
ap.add_argument("--outname", type=str, required=False,
                default=None, help="Output Filename (defaults to YEAR_MONTH.xlsx)")
args = ap.parse_args()


client = ZammadAPI(url=args.url, username=args.username, password=args.password)

if(args.year is None):
  args.year = datetime.now().year
 
if(args.month is None):
  args.month = datetime.now().month

Tickets = []
Times = []
Articles = []
Groups = [] 
result = client.time_accounting.by_ticket(args.year,args.month)

print("Got %i Tickets, loading times.." % len(result))

for ticket in result:
  id = ticket['ticket']['id']
  groupId = ticket['ticket']['group_id']
  if groupId not in [x['group_id'] for x in Groups]:
    print("Loading group %i for ticket %i" % (groupId, id))
    group = client.group.find(groupId)
    Groups.append({'group_name': group['name'], 'group_id': groupId})

  print("Loading times for ticket %i" % id)
  history = client.ticket_history.find(id)
  times = [
    { 'ticket_id': id,
      'time_id': x['id'],
      'created_at': x['created_at'],
      'm': datetime.fromisoformat(x['created_at'][:-1]).month,
      'y': datetime.fromisoformat(x['created_at'][:-1]).year,
      'created_by_id': x['created_by_id'],
      'time': float(0 if x['value_to'] == '' else x['value_to'])-float(0 if x['value_from'] == '' else x['value_from']) 
    } for x in history['history'] if 'attribute' in x and x['attribute'] == 'time_unit' and datetime.fromisoformat(x['created_at'][:-1]).month == args.month and datetime.fromisoformat(x['created_at'][:-1]).year == args.year
   ]

  articles = [
    { 
      'ticket_id': id,
      'article_id': x['id'],
      'created_at': x['created_at'],
      'body': x['body']
    } for key, x in history['assets']['TicketArticle'].items() 
  ]

  Times.extend(times)
  Articles.extend(articles)
  Tickets.append({
    'ticket_id': id,
    'number': ticket['ticket']['number'],
    'ticket_title': ticket['ticket']['title'],
    'customer': ticket['customer'],
    'organization': ticket['organization'],
    'ticket_agent': ticket['agent'],
    'group_id': ticket['ticket']['group_id'],
    'owner_id': ticket['ticket']['owner_id']
  })


Tickets =  pd.DataFrame(Tickets)
Tickets.set_index('ticket_id',inplace=True)

Times =  pd.DataFrame(Times)
Times.set_index('ticket_id',inplace=True)

Articles =  pd.DataFrame(Articles)
Articles.set_index('ticket_id',inplace=True)

Groups = pd.DataFrame(Groups)
Groups.set_index('group_id', inplace=True)

Times['created_at'] =  pd.to_datetime(Times['created_at'])
Articles['created_at'] =  pd.to_datetime(Articles['created_at'])

Articles['created_at'] = Articles['created_at'].dt.tz_localize(None)
Times['created_at'] = Times['created_at'].dt.tz_localize(None)

output = pd.merge_asof(
    Times.sort_values('created_at'),
    Articles.sort_values('created_at'),
    on="created_at",
    direction="backward",
    tolerance=pd.Timedelta(10, "s"),
    by='ticket_id').set_index('ticket_id').join(Tickets, rsuffix='_r').set_index('group_id').join(Groups)


if args.outname is None:
  args.outname = "%i_%i.xlsx" % (args.year, args.month)

print("Saving to %s" % args.outname)
if args.outname.endswith('xlsx'):
  output.to_excel(args.outname)
else:
  output.to_csv(args.outname)
