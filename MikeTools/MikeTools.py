from collections import OrderedDict
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import clr
clr.AddReference("DHI.Generic.MikeZero.DFS")
clr.AddReference("DHI.Mike1D.ResultDataAccess")
import DHI.Mike1D.ResultDataAccess as resultDataAccess
import DHI.Generic.MikeZero.DFS
import datetime as dt
import pandas as pd
import os.path

from pandas import DataFrame
from datetime import datetime

def __GetMetaData(quantity) -> dict:
	metaData = {}
	metaData["Type"] = quantity.get_ItemDescription()
	metaData["Unit"] = quantity.get_UnitAbbreviation()
	metaData["UnitNum"] = quantity.get_UnitAbbreviation().split(sep="/")[0]
	metaData["UnitDenom"] = quantity.get_UnitAbbreviation().split(sep="/")[1]
	metaData["UnitInt"] = quantity.get_UnitInt()
	metaData["Name"] = itemName
	metaData["ValueType"] = itemInfo.get_ValueType()
	metaData["ItemInt"] = quantity.get_ItemInt()

	return metaData;

def __ConvertSystemTimeToPyTime(sysTime,offset) -> dt.datetime:
	startTime = dt.datetime(year=sysTime.Year,
			month=sysTime.Month,
			day=sysTime.Day,
			hour=sysTime.Hour,
			minute=sysTime.Minute,
			second=sysTime.Second,
			microsecond=sysTime.Millisecond) + dt.timedelta(offset)

	return startTime;

class PrfFile:
	def __init__(self,filePath=None,readNow = True):
		self.filePath = filePath # type: str
		self.fileOpened = False # type: bool
		if readNow:
			self.OpenFile()
		else:
			self.fileData = None

	def OpenFile(self):
		assert isinstance(self.filePath,str), "filePath must be of type str. %s given" % type(self.filePath)
		assert os.path.isfile(self.filePath), "File does not exist: %s" % self.filePath
		self.fileData = DHI.Mike1D.ResultDataAccess.ResultData()
		self.fileData.Connection = DHI.Mike1D.Generic.Connection.Create(self.filePath)
		self.fileData.Load()
		self.fileOpened = True

	def GetListOfReachMUID(self) -> list:
		assert self.fileOpened, "Prf-file not Opened"
		noOfItems = self.fileData.Reaches.Count
		outputList = list() # type: list[str]
		for i in range(noOfItems):
			outputList.append(str(self.fileData.Reaches.get_Item(i).Id))
		return outputList

	def GetListOfNodeMUID(self) -> list:
		assert self.fileOpened, "Prf-file not Opened"
		noOfItems = self.fileData.Nodes.Count
		outputList = list() # type: list[str]
		for i in range(noOfItems):
			outputList.append(str(self.fileData.Nodes.get_Item(i).Id))
		return outputList

	def GetNumberOfNodes(self) -> int:
		assert self.fileOpened, "File not Opened"
		return int(self.fileData.Nodes.Count)

	def GetNumberOfReaches(self) -> int:
		assert self.fileOpened, "File not Opened"
		return int(self.fileData.Reaches.Count)

	def GetLevels(self,obsMUID,obsType,obsPlacement="Downstream") -> dict:
		assert obsType in ("Node","Reach"), "Observation type not recognized. Only Node and Reach available."
		assert str(type(self.fileData)) == "<class 'DHI.Mike1D.ResultDataAccess.ResultData'>", "Input file must be a ResultDataAccess instance"

		outputData = {} # type dict{str,float}
		if obsType == "Node":
			noOfItems = self.fileData.Nodes.Count

			# loop throug all wells in file
			for ii in range(noOfItems):

				reachName = str(self.fileData.Nodes.get_Item(ii).Id)

				# if well MUID matches MUID
				if reachName == obsMUID:
					outputData['bottomLevel'] = float(self.fileData.Nodes.get_Item(ii).BottomLevel)
					outputData['X'] = float(self.fileData.Nodes.get_Item(ii).get_XCoordinate())
					outputData['Y'] = float(self.fileData.Nodes.get_Item(ii).get_YCoordinate())
					outputData['groundLevel'] = float(self.fileData.Nodes.get_Item(ii).GroundLevel)

					break # break hvis den korrekte brønd er fundet i filen

				# Giv error hvis brønden ikke kunne findes i filen (dvs. for-loopet er kørt hele vejen igennem)
				else:
					raise Exception('Node not found: %s' % obsMUID)

		#### Discharge observations ####
		elif obsType == "Reach":
			noOfItems = self.fileData.Reaches.Count

			# loop through pipes
			for ii in range(noOfItems):

				reachName = str(self.fileData.Reaches.get_Item(ii).Id)[:len(obsMUID)]

				if reachName == obsMUID:
					outputData['diameter'] = self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(0).CrossSection.Diameter

					item = self.fileData.Reaches.get_Item(ii)

					counterItems = item.DataItems.Count # Number of points (H and Q) in pipe
					# Loop through points in pipe
					for iii in range(item.DataItems.Count):

						# If downstream data requested, search "backwards".
						if obsPlacement == "Downstream": 
							counterItems -= 1
						elif obsPlacement == "Upstream":
							counterItems = iii
						else:
							raise Exception('Unknown placement %s. Only Downstream and upstream available.' % obsPlacement)

						itemName = item.DataItems.get_Item(counterItems).Quantity.Id

						# If correct data type
						if itemName == "WaterLevel":	 
							outputData['bottomLevel'] = float(self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(counterItems).Z)
							outputData['X'] = float(self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(counterItems).X)
							outputData['Y'] = float(self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(counterItems).Y)
				
						break # Break if the correct point has been found

					else: # / loop through points in pipe
						raise Exception("Could not find data type %s i reach %s" % obsType, reachName)

					break # If the correct reach has been found

			else: # / loop through pipes
				raise Exception("Kunne ikke finde ledningen %s" % obsMUID)

		return outputData
	
	def GetData(self,obsMUID,obsType,obsPlacement="Downstream") -> pd.DataFrame:
		assert self.fileOpened, "Prf-file not Opened"
		assert str(type(self.fileData)) == "<class 'DHI.Mike1D.ResultDataAccess.ResultData'>", "Input file must be a ResultDataAccess instance"

		prfData = pd.DataFrame()
		metaData = {}

		#### Water level og depth observationer ####
		if obsType == "Waterlevel" or obsType == "Depth":	
			noOfItems = self.fileData.Nodes.Count

			# loop through nodes
			for ii in range(noOfItems):
				reachName = str(self.fileData.Nodes.get_Item(ii).Id)

				# Hvis brønd-navnet i filen matcher det angivne MUID
				if reachName == obsMUID:
					metaData["MUID"] = obsMUID
					metaData["Unit"] = "m"
					metaData["Type"] = obsType
					metaData["itemType"] = "Node"
					metaData["Placement"] = obsPlacement
					metaData["Chainage"] = None
					metaData["UnitNum"] = "m"
					metaData["UnitDenom"] = None

					# loop through time steps
					for time in range(self.fileData.TimesList.Count):
						datePrf = self.fileData.TimesList.get_Item(time)
						datePrf = datetime(datePrf.Year,datePrf.Month,datePrf.Day,datePrf.Hour,datePrf.Minute,datePrf.Second)
						x = self.fileData.Nodes.get_Item(ii).DataItems.get_Item(0).TimeData.get_Item(time)
				
							# Sikrer at output er af korrekt type
						if isinstance(x,list):
							x = float(x[0])
						elif str(type(x)) == "<class 'System.Double[]'>":
							x = float(x.Get())
						else:
							x = float(x)

						# Converting model data to depth if requested
						if obsType == "Depth":
							bottom = self.fileData.Nodes.get_Item(ii).BottomLevel
							if str(type(bottom)) == "<class 'System.Double[]'>":
								bottom = float(bottom.Get())

							x = x - bottom

						prfData = prfData.append([[datePrf,x]])
						# /loop through time steps

					break # break if the correct node has been found
						
			else: # /loop through nodes
				raise Exception('Node not found: %s' % obsMUID)

			# Set dataframe header
			prfData = prfData.set_index(0)
			if obsType == "Depth":
				prfData.columns = ["h_"+obsMUID+'_(m)']
			else:
				prfData.columns = ["H_"+obsMUID+'_(m)']

		#### Discharge observationer ####
		elif obsType == "Discharge" or obsType == "Pipe Waterlevel":
			noOfItems = self.fileData.Reaches.Count

			# loop gennem alle ledninger i filen
			for ii in range(noOfItems):

				reachName = str(self.fileData.Reaches.get_Item(ii).Id)[:len(obsMUID)]

				if reachName == obsMUID:
					metaData["MUID"] = obsMUID
					metaData["Type"] = obsType
					metaData["Placement"] = obsPlacement
					metaData["itemType"] = "Reach"

					item = self.fileData.Reaches.get_Item(ii)
					counterItems = item.DataItems.Count # Antal af datapunkter (H og Q) i den givne ledning

						# For hvert datapunkt i ledningen
					for iii in range(item.DataItems.Count):

						# Hvis der ønskes data fra længst nedstrøms, foretages søgningen "bagfra". Ellers forfra.
						if obsPlacement == "Downstream": 
							counterItems = counterItems-1
						elif obsPlacement == "Upstream":
							counterItems = iii
						else:
							raise Exception('ukendt placering %s' % obsPlacement)

						itemName = item.DataItems.get_Item(counterItems).Quantity.Id

						if obsType == "Discharge":
							if itemName == "Discharge" or itemName == "Pump Discharge (m3/s)":
								metaData["Unit"] = "m3/s"
								metaData["UnitNum"] = "m3"
								metaData["UnitDenom"] = "s"
								if itemName == "Discharge": 
									metaData["Type"] = "Discharge"
								else: 
									metaData["Type"] = "Pump Discharge"
								metaData["Chainage"] = float(item.GridPoints.get_Item(counterItems).Chainage)

								for time in range(self.fileData.TimesList.Count):
									x = item.DataItems.get_Item(counterItems).TimeData.GetValues(time).Get()
									datePrf = self.fileData.TimesList.get_Item(time)
									datePrf = datetime(datePrf.Year,datePrf.Month,datePrf.Day,datePrf.Hour,datePrf.Minute,datePrf.Second)
									prfData=prfData.append([[datePrf,x]])

								break

						else:
							if itemName == "WaterLevel":	 

								for time in range(self.fileData.TimesList.Count):
									x = item.DataItems.get_Item(counterItems).TimeData.GetValues(time).Get()
									datePrf = self.fileData.TimesList.get_Item(time)
									datePrf = datetime(datePrf.Year,datePrf.Month,datePrf.Day,datePrf.Hour,datePrf.Minute,datePrf.Second)
									prfData=prfData.append([[datePrf,x]])
									metaData["Type"] = "WaterLevel"
									metaData["Unit"] = "m"
									metaData["UnitNum"] = "m"
									metaData["UnitDenom"] = None
									metaData["Chainage"] = float(item.GridPoints.get_Item(counterItems).Chainage)
								break

					else: 
						raise Exception("Kunne ikke finde datatypen Discharge i ledningen %s" % reachName)

					break

			else: # Hvis for-loopet er kørt til vejs ende, ledningen ikke kunne findes
				raise Exception("Kunne ikke finde ledningen %s" % obsMUID)

				prfData = prfData.set_index(0)
				if obsType == "Discharge":
					prfData.columns = ["Q_"+obsMUID+'_(m3/s)']
				else:
					prfData.columns = ["h_"+obsMUID+'_(m)']

		else: raise Exception("Observation type not supported: %s" % obsType)

		prfData[prfData.columns[0]].metaData = metaData

		return prfData

	def ReadData(self,obsMUID,obsType,obsPlacement="Downstream"):
		assert isinstance(obsMUID,(str,list,tuple)), "obsMUID must be a string, list or tuple. %s given" % type(obsMUID)
		assert isinstance(obsType,(str,list,tuple)), "obsMUID must be a string, list or tuple. %s given" % type(obsType)
		assert type(obsMUID) == type(obsType), "obsMUID and obsType must be of same type. %s and %s given" % (type(obsMUID), type(obsType))
		if isinstance(obsMUID,list):
			assert len(obsMUID) == len(obsType), "obsMUID and obsType must be of equal length"
		if isinstance(obsPlacement,list):
			assert len(obsPlacement) == len(obsMUID), "obsPlacement must be same length as obsMUID and obsType"

		if isinstance(obsMUID,str):
			obsMUID = [obsMUID]
			obsType = [obsType]
		if isinstance(obsPlacement,str):
			obsPlacement = [obsPlacement] * len(obsMUID)

		self.outputData = OutputData()
		for itemCounter,item in enumerate(obsMUID):
			data=self.GetData(item,obsType[itemCounter],obsPlacement[itemCounter])
			if itemCounter == 0:
				self.outputData.data = data[0]
			else:
				self.outputData.data = self.outputData.data.join(data[0])
			self.outputData.metaData[data[0].columns[0]] = data[1]
			self.outputData.noOfItems += 1

class DataPlotter:
	def __init__(self,OutputData=None,startTime=None,endTime=None):
		self.outputData = outputData
		self.startTime = startTime
		self.endTime = endTime

	def PlotTS(self,items="All",legendOn=True,legendTypesOn=True,legendUnitsOn=False):

		###### AxisAndLegend
		def AxisAndLegend(legendOn,legendTypesOn,legendUnitsOn):
			typesTemp=set()
			legend = []
			for itemName in self.outputData.data:
				metaData = self.outputData.metaData[itemName]
				typesTemp.add(metaData["Type"] + " (" + metaData["Unit"] + ")")
				if legendOn:
					legendText = metaData["MUID"]
					if legendTypesOn:
						legendText = legendText + " " + metaData["Type"] 
					if legendUnitsOn:
						legendText = legendText + " (" + metaData["Unit"] + ")"
					legend.append(legendText)
				else:
					legend=None

			yAxisText = ', '.join(map(str, list(typesTemp)))
			return (yAxisText,legend)
		####### AxisAndLegend
		
		self.tsPlot = plt.figure()
		plt.plot(self.outputData)
		yAxisText,legend = AxisAndLegend(legendOn,legendTypesOn,legendUnitsOn)

		plt.ylabel(yAxisText)
		plt.legend(legend)
		plt.show()

class dfs0File:
	def __init__(self,filePath=None,openNow=True,loadNow=True):
		self.filePath = filePath
		self._fileOpen = False
		self.fileData = None
		self._fileDataLoaded = False

		if openNow:
			self.OpenFile()

		if loadNow:
			self.LoadData()

	def OpenFile(self):
		assert isinstance(self.filePath,str), "filePath must be of type str"
		assert not self._fileOpen, "File is already open"
		self.fileConn= DfsFileFactory.DfsGenericOpen(self.filePath)
		self._fileOpen = True

	def CloseFileConn(self):
		assert self._fileOpen, "File is not open"
		self.fileConn.Close()

	def LoadData(self):
		assert self._fileOpen, "File is not open"
		self.fileData = self.ReadData()

	def ReadData(self,items="All"):
		assert isinstance(items,(str,list,tuple)), "items must be of either types string, list, tuple"
		assert self._fileOpen, "File must first be opened"
		assert self.fileConn.FileInfo.TimeAxis.get_TimeUnit() == 1400, \
		"Program hardcoded to seconds. TimeUnit code in file: %s" % self.fileData.FileInfo.TimeAxis.get_TimeUnit()
		
		outputData = pd.DataFrame()

		startTime = ConvertSystemTimeToPyTime(self.fileConn.FileInfo.TimeAxis.get_StartDateTime(),
									seconds=self.fileConn.FileInfo.TimeAxis.get_StartTimeOffset())

		for i in range(1,self.fileConn.ItemInfo.Count+1):
			itemInfo = self.fileConn.ItemInfo.get_Item(i-1)

			itemName = itemInfo.get_Name()
			if itemName in items or items == "All":
				outputDataList = list()

				metaData = GetMetaData(itemInfo.get_Quantity())

				for t in range(self.fileConn.FileInfo.TimeAxis.get_NumberOfTimeSteps()):
					dataSys = self.fileConn.ReadItemTimeStep(i,t)
					data = dataSys.Data.Get()
					outputDataList.append(data)
				
					if i == 1:
						time = startTime + dt.timedelta(seconds=int(dataSys.get_Time()))
						outputData = outputData.append([[time,data]])
	
				if i == 1:
					outputData = outputData.set_index(0)
					outputData.columns = [itemName]

				else:
					outputData[itemName] = outputDataList

				outputData[itemName].metaData=metaData

		return outputData

class Dfs23File:
	def __init__(self,filePath=None,openNow=True,loadNow=True):
		self.filePath = filePath
		self._fileOpen = False
		self.fileData = None
		self.fileDataLoaded = False

		if openNow:
			self.OpenFile()

		if loadNow:
			self.LoadData()

	def OpenFile(self):
		assert isinstance(self.filePath,str), "filePath must be of type str"
		assert not self._fileOpen, "File is already open"
		self.fileConn= DfsFileFactory.DfsGenericOpen(self.filePath)
		self.fileOpen = True

	def CloseFileConn(self):
		assert self._fileOpen, "File is not open"
		self.fileConn.Close()

	def LoadData(self):
		assert self._fileOpen, "File is not open"
		self.fileData = self.ReadData()

	def ReadItemTimeStep(self,item,timestep):
		dataSys = self.fileConn.ReadItemTimeStep(item,timestep)
					
		if self.zCount == 0:
			data = np.array(list(dataSys.get_Data())).reshape((self.yCount,self.xCount,1,1))
		else:
			data = np.array(list(dataSys.get_Data())).reshape((self.yCount,self.xCount,self.zCount,1))

		if i == 1:
			time = startTime + dt.timedelta(seconds=int(dataSys.get_Time()))

		return (time,data)

	def ReadData(self,items="All"):
		assert isinstance(items,(str,list,tuple)), "Items must be of either types string, list, tuple"
		assert self._fileOpen, "File must first be opened"
		assert self.fileConn.FileInfo.TimeAxis.get_TimeUnit() == 1400, \
			"Program hardcoded to seconds. TimeUnit code in file: %s" % self.fileData.FileInfo.TimeAxis.get_TimeUnit()
		
		startTime = ConvertSystemTimeToPyTime(self.fileConn.FileInfo.TimeAxis.get_StartDateTime(),
									seconds=self.fileConn.FileInfo.TimeAxis.get_StartTimeOffset())

		self.xCount = self.fileConn.ItemInfo[0].SpatialAxis.XCount
		self.yCount = self.fileConn.ItemInfo[0].SpatialAxis.YCount

		if self.fileConn.ItemInfo[0].SpatialAxis.Dimension == 3:
			self.zCount = self.fileConn.ItemInfo[0].SpatialAxis.zCount
			outputData = np.empty((self.yCount,self.xCount,self.zCount,0))
		else:
			self.zCount = 0
			outputData = np.empty((self.yCount,self.xCount,1,0))
			
		times = list()

		for i in range(1,self.fileConn.ItemInfo.Count+1):
			itemInfo = self.fileConn.ItemInfo.get_Item(i-1)
			itemName = itemInfo.get_Name()
			if itemName in items or items == "All":

				metaData = GetMetaData(itemInfo.get_Quantity())

				for t in range(self.fileConn.FileInfo.TimeAxis.get_NumberOfTimeSteps()):
					time,data = ReadItemTimeStep(i,t)
					outputData = np.append(data,data,axis=3)

					if i == 1:
						times.append(time)

		return (times,outputData,metaData)

if __name__ == '__main__':
	#dfs2File = Dfs23File(filePath = "D:\Arbejdsmappe\Karup_ET_UzCells_obs_Monthly.dfs2")
	#dfs3File = Dfs23File(filePath = "KarupUZ_twin_Monthly_NoUncertainty.dfs3")
	prfFile = PrfFile(".\TestData\Test.PRF")
	nodeMUIDs = prfFile.GetListOfNodeMUID()
	reachMUIDs = prfFile.GetListOfReachMUID()
	noOfNodes = prfFile.GetNumberOfNodes()
	noOfReaches = prfFile.GetNumberOfReaches()

	#nodeLevels = prfFile.GetLevels('0145AC1','Node')
	#reachLevels = prfFile.GetLevels('1773HH9-7785FB171','Reach')

	nodeData = prfFile.GetData('0145AC1','Waterlevel')
	reachData = prfFile.GetData('1773HH9-7785FB171','Discharge')

	#assert len(nodeMUIDs) == noOfNodes
	#assert len(reachMUIDs) == noOfReaches

	
 	pass