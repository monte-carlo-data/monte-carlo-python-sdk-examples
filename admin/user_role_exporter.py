from pycarlo.core import Client, Query, Mutation, Session
import csv

def userRoleExporter(file_name):
	with open(file_name, "w") as user_roles:
		csv_writer=csv.writer(user_roles)
		first_row=["Email","Role", "Status"]
		csv_writer.writerow(first_row)

		query = Query()
		user_roles = query.get_user.account
		user_roles.users(first=5000).edges.node.__fields__('email','role','permissions','state')
		user_roles.user_invites(first=5000,state="sent").edges.node.__fields__('email','state','role')
		print(query)

		user_invites=client(query).get_user.account.user_invites.edges
		user_roles=client(query).get_user.account.users.edges

		for user in user_roles:
			csv_writer.writerow([user.node.email,user.node.role,"ACTIVE"])
		for user in user_invites:
			csv_writer.writerow([user.node.email,user.node.role,"INVITED"])

if __name__ == '__main__':
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	client = Client(session=Session(mcd_id=mcd_id,mcd_token=mcd_token))
	userRoleExporter('user_roles.csv')
