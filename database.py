METADATA = "metadata.txt"
import collections
import csv

class Database(object) :
	"""docstring for database"""
	def __init__(self) :
		self.name = "Database"
		self.tables = {}

	def initializeData(self) :
		metadata = open(METADATA, "r")
		lines = metadata.readlines()
		numOFLines = len(lines)
		i = 0
		while i < numOFLines :
			line = lines[i].rstrip("\r\n")
			if line == "<begin_table>" :
				i += 1
				table = lines[i].rstrip("\r\n")
				self.tables[table] = {}
				self.tables[table]["columns"] = {}
				i += 1
				col = lines[i].rstrip("\r\n")
				j = 0
				while col != "<end_table>" :
					self.tables[table]["columns"][table+"."+col] = j
					i += 1
					j += 1
					col = lines[i].rstrip("\r\n")
				i += 1

	def populateTables(self) :
		tables = self.tables.keys()
		for table in tables :
			fileName = "files/" + table + ".csv"
			file = open(fileName, "r")
			rows = csv.reader(file)
			self.tables[table]["data"] = []
			for row in rows :
				self.tables[table]["data"].append(row)
