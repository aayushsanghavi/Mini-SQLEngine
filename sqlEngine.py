from database import *
import re

class SQLEngine(object) :
	"""docstring for SQLEngine"""
	def __init__(self) :
		self.queryString = ""
		self.answer = collections.OrderedDict()
		self.splitString = []
		self.selectArgs = []
		self.fromArgs = []
		self.whereArgs = ""
		self.joinedColumns = {}
		self.joinedTables = []
		self.error = False

	def getArgs(self, database) :
		string = re.match(r"select(.*)from", self.queryString)
		if string :
			self.selectArgs = re.sub("select\s+", "", string.group(1))
			self.selectArgs = self.selectArgs.split(",")
			for i in range(len(self.selectArgs)) :
				self.selectArgs[i] = self.selectArgs[i].lstrip().rstrip()
		else :
			self.showError()

		string = re.match(r"(.*)from(.*)", self.queryString)
		if string :
			if "where" in string.group(2) :
				string = re.match(r"(.*)from(.*)where", self.queryString)
				if string :
					self.fromArgs = re.sub("from\s+", "", string.group(2))
					self.fromArgs = self.fromArgs.split(",")
					string = re.match(r"(.*)where(.*)", self.queryString)
					self.whereArgs = re.sub("where\s+", "", string.group(2))
				else :
					self.showError()
			else :
				self.fromArgs = re.sub("from\s+", "", string.group(2))
				self.fromArgs = self.fromArgs.split(",")

			for i in range(len(self.fromArgs)) :
				self.fromArgs[i] = self.fromArgs[i].lstrip().rstrip()
		else :
			self.showError()

		if len(self.fromArgs) > 1 :
			self.joinedTables = self.joinTables(database.tables[self.fromArgs[0]], database.tables[self.fromArgs[1]])

	def showError(self) :
		self.error = True
		print "Error in query syntax - Not an SQL query"

	def checkCondition(self, columnValue, condition, value) :
		columnValue = columnValue.lstrip().rstrip()
		condition = condition.lstrip().rstrip()
		value = value.lstrip().rstrip()

		columnValue = int(columnValue)
		value = int(value)

		if condition == "=" :
			if columnValue == value :
				return True
		elif condition == ">=" :
			if columnValue >= value :
				return True
		elif condition == "<=" :
			if columnValue <= value :
				return True
		elif condition == ">" :
			if columnValue > value :
				return True
		elif condition == "<" :
			if columnValue < value :
				return True
		elif condition == "!=" :
			if columnValue != value :
				return True
		else :
			return False

	def joinTables(self, table1, table2) :
		results = []
		self.joinedColumns.update(table1["columns"])
		index = len(table1["columns"])
		for  key in table2["columns"] :
			self.joinedColumns[key] = table2["columns"][key] + index

		for i in table1["data"] :
			for j in table2["data"] :
				results.append(i + j)
		return results

	def evaluateWhere(self, database) :
		results = []
		if len(self.fromArgs) == 1 :
			if not "and" in self.whereArgs and not "or" in self.whereArgs :
				column, condition, value = self.whereArgs.split()
				for row in database.tables[self.fromArgs[0]]["data"] :
					if self.checkCondition(row[database.tables[self.fromArgs[0]]["columns"][column]], condition, value) :
						results.append(row)
			else :
				if "and" in self.whereArgs :
					logic = "and"
				else :
					logic = "or"

				conditions = re.split(logic, self.whereArgs)
				column1, condition1, value1 = conditions[0].split()
				column2, condition2, value2 = conditions[1].split()
				for row in database.tables[self.fromArgs[0]]["data"] :
					if logic == "and" :
						if self.checkCondition(row[database.tables[self.fromArgs[0]]["columns"][column1]], condition1, value1) and self.checkCondition(row[database.tables[self.fromArgs[0]]["columns"][column2]], condition2, value2) :
							results.append(row)
					else :
						if self.checkCondition(row[database.tables[self.fromArgs[0]]["columns"][column1]], condition1, value1) or self.checkCondition(row[database.tables[self.fromArgs[0]]["columns"][column2]], condition2, value2) :
							results.append(row)

		else :
			if not ("and" in self.whereArgs or "or" in self.whereArgs) :
				column, condition, value = self.whereArgs.split()
				for row in self.joinedTables :
					if self.checkCondition(row[database.tables[table]["columns"][column]], condition, value) :
						results.append(row)
			else :
				if "and" in self.whereArgs :
					logic = "and"
				else :
					logic = "or"

				conditions = re.split(logic, self.whereArgs)
				column1, condition1, value1 = conditions[0].split()
				column2, condition2, value2 = conditions[1].split()
				for row in self.joinedTables :
					if logic == "and" :
						if self.checkCondition(row[self.joinedColumns[column1]], condition1, value1) and self.checkCondition(row[self.joinedColumns[column2]], condition2, value2) :
							results.append(row)
					else :
						if self.checkCondition(row[self.joinedColumns[column1]], condition1, value1) or self.checkCondition(row[self.joinedColumns[column2]], condition2, value2) :
							results.append(row)

		return results

	def checkError(self, database) :
		if len(self.fromArgs) == 1 and len(self.selectArgs) >= 1 and self.selectArgs[0] != "*" :
			for column in self.selectArgs :
				if column not in database.tables[self.fromArgs[0]]["columns"] :
					print "Error in column name - Column " + column + " doesn't exist in table " + self.fromArgs[0]
					self.error = True
		elif len(self.fromArgs) > 1 and len(self.selectArgs) >= 1 and self.selectArgs[0] != "*" :
			for arg in self.selectArgs :
				table, column = arg.split(".")
				table = table.rstrip().lstrip()
				column = column.rstrip().lstrip()
				if arg not in database.tables[table]["columns"] :
					print "Error in column name - Column " + column + " doesn't exist in table " + table
					self.error = True
					return

	def selectQuery(self, results, columns) :
		if len(self.selectArgs) == 1 and "*" in self.selectArgs[0] :
			return results
		else :
			selectedResults = []
			if len(self.fromArgs) == 1 :
				for row in results :
					temp = []
					for column in self.selectArgs :
						temp.append(row[columns[column]])
					selectedResults.append(temp)
			else :
				for row in results :
					temp = []
					for column in self.selectArgs :
						temp.append(row[columns[column]])
					selectedResults.append(temp)
			
			return selectedResults

def main() :
	database = Database()
	database.initializeData()
	database.populateTables()

	sqlEngine = SQLEngine()
	while True :
		sqlEngine.error = False
		queryString = raw_input("SQL> ")
		if queryString == "exit" :
			print "Bye"
			break
		
		if queryString[-1] != ";" :
			print "Error in query syntax - Missing ;"
			sqlEngine.error = True
		else :
			queryString = queryString[:-1]
			sqlEngine.queryString = queryString
			sqlEngine.getArgs(database)
			sqlEngine.checkError(database)
			crudeResults = []
			columns = []
			if not sqlEngine.error :
				if len(sqlEngine.fromArgs) > 1 and sqlEngine.whereArgs != "" :
					crudeResults = sqlEngine.evaluateWhere(database)
					columns = sqlEngine.joinedColumns
				elif len(sqlEngine.fromArgs) > 1 and sqlEngine.whereArgs == "" :
					crudeResults = sqlEngine.joinedTables
					columns = sqlEngine.joinedColumns
				else :
					crudeResults = sqlEngine.evaluateWhere(database)
					columns = database.tables[sqlEngine.fromArgs[0]]["columns"]

			refined = sqlEngine.selectQuery(crudeResults, columns)
			print refined
			print len(refined)

if __name__ == "__main__" :
	main()