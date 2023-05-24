from pycarlo.core import Client, Query, Mutation, Session
import csv

def userRoleExporter(file_name):
	with open(file_name, "w") as roles:
		csv_writer=csv.writer(roles)
		first_row=["Email","Role"]
		csv_writer.writerow(first_row)

		user_query = '''
		query {
		  getUsersInAccount(first: 1000) {
		    pageInfo {
		      hasNextPage
		      endCursor
		    }
		    edges {
		      node {
		        email
		        auth {
		          groups
		        }
		      }
		    }
		  }
		}
		'''
		query=Query()
		response= client(user_query).get_users_in_account.edges

		for user in response:
			print(user.node.email)
			csv_writer.writerow([user.node.email,str(user.node.auth.groups)])

if __name__ == '__main__':
	mcd_id = input("MCD ID: ")
	mcd_token = input("MCD Token: ")
	csv_name = input("CSV Name: ")
	client = Client(session=Session(mcd_id=mcd_id,mcd_token=mcd_token))
	userRoleExporter(csv_name)
