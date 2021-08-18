'''
Requirements : 
1) The model has to run @ end of every month
	The updater scipt calls the model whenevr it is the end of the month
	1) That will acess main db and then take the data 
	2) Make prediciton for the next month 
2) Store the prediction in predicion table
'''


#import
import sqlite3  
import pandas as pd 
import pmdarima as pm
import datetime
from dateutil.relativedelta import relativedelta

def checking_connection(db_loc):
	#establishing a connection 
	conn = sqlite3.connect(db_loc)
	conn.close()
	return (True)


def executing_command(command, db_loc):
	if checking_connection(db_loc):
		conn = sqlite3.connect(db_loc)
		conn.execute(command)
		conn.commit()
		conn.close()


def selecting_data_for_model(db_loc, table_name, dict_col_and_types):
	if (checking_connection(db_loc)):
		#first checking the last row ID so we will check whether it is greater than 72 as 
		#we have assumed max_P to be 6 and max_Q to be 2 as 3*2 = 6 years 
		#if it is lesss thand 72 we will take max_P and max_Q to be what ever it is possible
		
		# finding the size of the database
		command = "SELECT ID FROM {} ".format(table_name) 
		command += "ORDER BY ID "
		command += "DESC LIMIT 1 ;"
		conn = sqlite3.connect(db_loc)
		curser = conn.execute(command)
		for row in curser:
			size = row[0]
		conn.close()
		# selecting the max_P and max_Q based on the data 
		if size > 72: 
			max_P = 6 
			max_Q = 2
		elif size <= 72:
			max_P = (size) // 12
			max_Q  = (size) // (3 * 12)

		# the noofrows that we are going to take is max_P * 12 
		noofrows = max_P * 12 
		command = "SELECT * FROM {} WHERE ID IN (SELECT ID FROM {} ORDER BY ID DESC LIMIT {}) ORDER BY ID ASC".format(table_name, table_name,noofrows)
		conn = sqlite3.connect(db_loc)
		curser = conn.execute(command)
		data_tuple = []
		for row in curser:
			data_tuple.append(row)
		conn.close()
		df = pd.DataFrame(data_tuple, columns= list(dict_col_and_types.keys()))
		df.drop("ID", axis = 1, inplace = True) 
		df['MONTH'] = pd.to_datetime(df['MONTH'], format = "%Y-%m")
		df.set_index("MONTH", inplace = True)
		return (df, max_P, max_Q)


def predict_next_month(df, max_P, max_Q):
	l = df.shape[1]
	day = df.iloc[-1, :].reset_index().columns[1] 
	day += relativedelta(months = 1)
	day = str(day.strftime("%Y-%m"))
	dict_col_and_predictions = {}
	dict_col_and_predictions["MONTH"] = str('\'') + str(day) + str('\'')
	predictions = []
	for i in range(l):
		train_df = df.iloc[:, i]
		stepwise_fit = pm.auto_arima(train_df, start_p=0, start_q=0,
	                             max_p = 11, max_q= 11, m=12,
	                             start_P=0, start_Q = 0,seasonal=True,
	                             max_P = max_P, 
	                             max_Q = max_Q,
	                             error_action='ignore',  # don't want to know if an order does not work
	                             suppress_warnings=True,  # don't want convergence warnings
	                             stepwise=True)  # set to stepwise_fit
		predictions.append(stepwise_fit.predict(n_periods = 1))
	i = 0
	for col in df.columns:
		dict_col_and_predictions[col] = predictions[i][0]
		i = i + 1
	return (dict_col_and_predictions)


def create_insert_command(table_name, dict_col_and_values):
	command = "INSERT INTO {} ".format(table_name)
	command += " ("
	for col in dict_col_and_values.keys():
		command += str(col) + str(" ,")
	command = command[: -1]
	command += ") "
	command += "VALUES" 
	command += "("	
	for col in dict_col_and_values.keys():
		command += str(dict_col_and_values[col]) + str(" ,")
	command = command[: -1]
	command += ")"
	command += ";"
	return command


def run_model():

	print("Running the model -- ")
	db_loc = r'C:\Users\chand\Documents\P\Projects\Locally_personalised_ads\db\payment_db.db'
	table_name_1 = "tab1"
	dict_col_and_types = {	"ID" : "INT PRIMARY KEY AUTOINCREMENT",
							"MONTH" : "TEXT NOT NULL",  #can we have seprate columns as years and month
							"FOOD" : "REAL NOT NULL", 
							"FUEL" : "REAL NOT NULL"} 
	print("selecting the data for model -- ")
	(df, max_P, max_Q) = selecting_data_for_model(db_loc, table_name_1, dict_col_and_types)
	print("prediciting the next month -- ")
	dict_col_and_predictions =  predict_next_month(df, max_P, max_Q)

	table_name_2 = "tab2"

	command = create_insert_command(table_name_2, dict_col_and_predictions)
	#print(command)
	print("Inserting data in to tab2")
	executing_command(command, db_loc)

if __name__ == "__main__":
	run_model()


#SQL COMMAND FOR CREATING TAB2 
# CREATE TABLE tab2(
# 	ID INTEGER PRIMARY KEY AUTOINCREMENT,
# 	MONTH TEXT NOT NULL,
# 	FOOD REAL NOT NULL,
# 	FUEL REAL NOT NULL
# 	);

# CREATE TABLE tab3(
# 	FOOD REAL NOT NULL,
# 	FUEL REAL NOT NULL
# 	);
