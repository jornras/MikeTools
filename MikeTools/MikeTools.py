from collections import OrderedDict
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

import clr
clr.AddReference("DHI.Generic.MikeZero.DFS")
clr.AddReference("DHI.Mike1D.ResultDataAccess")
import DHI.Mike1D.ResultDataAccess
from DHI.Generic.MikeZero.DFS import *
from DHI.Generic.MikeZero.DFS.dfs0 import *
import datetime as dt

class OutputData:
      def __init__(self):
            class MetaData(OrderedDict):
                  def __init__(self):
                        pass

                  def __setitem__(self, key, value):
                        if key in self:
                              del self[key]
                        OrderedDict.__setitem__(self, key, value)

            self.data = pd.DataFrame()
            self.metaData = MetaData()
            self.noOfItems=0

class PrfFile:
      def __init__(self,filePath=None,readNow = True):
            self.filePath = filePath
            self.fileLoaded = False
            if readNow:
                  self.OpenPrf()
            else:
                  self.fileData = None
                  

      def OpenPrf(self):
          assert isinstance(self.filePath,str), "filePath must be of type str. %s given" % type(self.filePath)
          
          self.fileData = DHI.Mike1D.ResultDataAccess.ResultData()
          self.fileData.Connection = DHI.Mike1D.Generic.Connection.Create(self.filePath)
          self.fileData.Load()
          self.fileLoaded = True

      def GetLevels(self,obsMUID,obsType,obsPlacement="Downstream"):
          assert str(type(self.fileData)) == "<class 'DHI.Mike1D.ResultDataAccess.ResultData'>", "Input file must be a ResultDataAccess instance"

          obsData = {}
          if obsType == "Node":
              obsData['obsType'] = 'Node'
              noOfItems = self.fileData.Nodes.Count

		      # loop gennem alle brønde i filen
              for ii in range(noOfItems):

                  reachName = str(self.fileData.Nodes.get_Item(ii).Id)

			      # Hvis brønd-navnet i filen matcher det angivne MUID
                  if reachName == obsMUID:
                      obsData['bottomLevel'] = float(self.fileData.Nodes.get_Item(ii).BottomLevel)
                      obsData['X'] = float(self.fileData.Nodes.get_Item(ii).get_XCoordinate())
                      obsData['Y'] = float(self.fileData.Nodes.get_Item(ii).get_YCoordinate())
                      obsData['groundLevel'] = float(self.fileData.Nodes.get_Item(ii).GroundLevel)

                      break # break hvis den korrekte brønd er fundet i filen

		      # Giv error hvis brønden ikke kunne findes i filen (dvs. for-loopet er kørt hele vejen igennem)
              else:
                  raise Exception('Knude ikke fundet: %s' % obsMUID)

	      #### Discharge observationer ####
          elif obsType == "Reach":
              obsData['obsType'] = 'Reach'
              noOfItems = self.fileData.Reaches.Count

		      # loop gennem alle ledninger i filen
              for ii in range(noOfItems):

                  reachName = str(self.fileData.Reaches.get_Item(ii).Id)[:len(obsMUID)]

                  if reachName == obsMUID:
                      obsData['diameter'] = self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(0).CrossSection.Diameter

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

					      # Hvis datatypen er af typen Q

                          if itemName == "WaterLevel":     
                              obsData['bottomLevel'] = float(self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(counterItems).Z)
                              obsData['X'] = float(self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(counterItems).X)
                              obsData['Y'] = float(self.fileData.Reaches.get_Item(ii).get_GridPoints().get_Item(counterItems).Y)
                        
                              break
                      else: # Hvis for-loopet er kørt til vejs ende, betyder det at ledningen ikke har data af typen discharge
                          raise Exception("Kunne ikke finde datatypen Water level i ledningen %s" % reachName)

                      break

              else: # Hvis for-loopet er kørt til vejs ende, ledningen ikke kunne findes
                  raise Exception("Kunne ikke finde ledningen %s" % reachName)

          return obsData

      def GetData(self,obsMUID,obsType,obsPlacement="Downstream"):
          assert self.fileLoaded, "Prf-file not loaded"
          assert str(type(self.fileData)) == "<class 'DHI.Mike1D.ResultDataAccess.ResultData'>", "Input file must be a ResultDataAccess instance"
          from pandas import DataFrame
          from datetime import datetime
  
    
          prfData = DataFrame()
          metaData = {}

	      #### Water level og depth observationer ####
          if obsType == "Waterlevel" or obsType == "Depth":	
              noOfItems = self.fileData.Nodes.Count

		      # loop gennem alle brønde i filen
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

				      # For hvert tidsskridt i filen
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

					      # Konvertering af modelresultat hvis observationen er af typen depth
                          if obsType == "Depth":
                              bottom = self.fileData.Nodes.get_Item(ii).BottomLevel
                              if str(type(x)) == "<class 'System.Double[]'>":
                                  bottom = float(bottom.Get())

                              x = x - bottom

                          prfData = prfData.append([[datePrf,x]])

                      break # break hvis den korrekte brønd er fundet i filen

		      # Giv error hvis brønden ikke kunne findes i filen (dvs. for-loopet er kørt hele vejen igennem)
              else:
                  raise Exception('Knude ikke fundet: %s' % obsMUID)

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

					      # Hvis datatypen er af typen Q
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
                      else: # Hvis for-loopet er kørt til vejs ende, betyder det at ledningen ikke har data af typen discharge
                          raise Exception("Kunne ikke finde datatypen Discharge i ledningen %s" % reachName)

                      break

              else: # Hvis for-loopet er kørt til vejs ende, ledningen ikke kunne findes
                  raise Exception("Kunne ikke finde ledningen %s" % reachName)

              prfData = prfData.set_index(0)
              if obsType == "Discharge":
                    prfData.columns = ["Q_"+obsMUID+'_(m3/s)']
              else:
                    prfData.columns = ["h_"+obsMUID+'_(m)']

          else: raise Exception("Observation type not supported %s" % obsType)

          return (prfData,metaData)

      def ReadData(self,obsMUID,obsType,obsPlacement="Downstream"):
            assert isinstance(obsMUID,(str,list,tuple)), "obsMUID must be a string, list or tuple. %s given" % type(obsMUID)
            assert isinstance(obsType,(str,list,tuple)), "obsMUID must be a string, list or tuple. %s given" % type(obsType)
            assert type(obsMUID) == type(obsType), "obsMUID and obsType must be of same type. %s and %s given" % (type(obsMUID), type(obsType))
            if isinstance(obsMUID,list):
                  assert len(obsMUID) == len(obsType), "obsMUID and obsType must be of equal length"
            if isinstance(obsPlacement,list):
                  assert len(obsPlacement) == len(obsMUID), "obsPlacement must be same length as obsMUID and obsType"

            import pandas as pd

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

      def PlotDataFrame(self,legend=None):
          import matplotlib.pyplot as plt
          fig = plt.figure()
          dataFrame.interpolate()
          plt.plot(dataFrame)
          if "Discharge" in str(dataFrame.dtypes.index[0]):
              plt.ylabel('Discharge (m3/h)')
          elif "Level" in str(dataFrame.dtypes.index[0]):
              plt.ylabel('Level (m)')
          elif "Depth" in str(dataFrame.dtypes.index[0]):
              plt.ylabel('Depth (m)')
          plt.grid(True)
          if legend != None:
              plt.legend(legend)
          plt.show()

class ResultPlotter:
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
            self.fileOpen = False
            self.fileData = None
            self.fileDataLoaded = False

            if openNow:
                  self.OpenFile()

            if loadNow:
                  self.LoadData()

      def OpenFile(self):
            assert isinstance(self.filePath,str), "filePath must be of type str"
            assert not self.fileOpen, "File is already open"
            self.fileConn= DfsFileFactory.DfsGenericOpen(self.filePath)
            self.fileOpen = True

      def CloseFileConn(self):
            assert self.fileOpen, "File is not open"
            self.fileConn.Close()

      def LoadData(self):
            assert self.fileOpen, "File is not open"
            self.fileData = self.ReadData()

      def ReadData(self,items="All"):
            assert isinstance(items,(str,list,tuple)), "items must be of either types string, list, tuple"
            assert self.fileOpen, "File must first be opened"
            assert self.fileConn.FileInfo.TimeAxis.get_TimeUnit() == 1400, "Program hardcoded to seconds. TimeUnit code in file: %s" % self.fileData.FileInfo.TimeAxis.get_TimeUnit()
            
            outputData = pd.DataFrame()

            startTimeSys = self.fileConn.FileInfo.TimeAxis.get_StartDateTime()
            startTime = dt.datetime(year=startTimeSys.Year,month=startTimeSys.Month,day=startTimeSys.Day,hour=startTimeSys.Hour,minute=startTimeSys.Minute,second=startTimeSys.Second,microsecond=startTimeSys.Millisecond) + dt.timedelta(seconds=self.fileConn.FileInfo.TimeAxis.get_StartTimeOffset())
            counter=0

            for i in range(1,self.fileConn.ItemInfo.Count+1):
                  itemInfo = self.fileConn.ItemInfo.get_Item(i-1)

                  itemName = itemInfo.get_Name()
                  if itemName in items or items == "All":
                        outputDataList = list()

                        metaData = {}
                        
                        quantity = itemInfo.get_Quantity()
                        metaData["Type"] = quantity.get_ItemDescription()
                        metaData["Unit"] = quantity.get_UnitAbbreviation()
                        metaData["UnitNum"] = quantity.get_UnitAbbreviation().split(sep="/")[0]
                        metaData["UnitDenom"] = quantity.get_UnitAbbreviation().split(sep="/")[1]
                        metaData["UnitInt"] = quantity.get_UnitInt()
                        metaData["Name"] = itemName
                        metaData["ValueType"] = itemInfo.get_ValueType()
                        metaData["ItemInt"] = quantity.get_ItemInt()

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
            self.fileOpen = False
            self.fileData = None
            self.fileDataLoaded = False

            if openNow:
                  self.OpenFile()

            if loadNow:
                  self.LoadData()

      def OpenFile(self):
            assert isinstance(self.filePath,str), "filePath must be of type str"
            assert not self.fileOpen, "File is already open"
            self.fileConn= DfsFileFactory.DfsGenericOpen(self.filePath)
            self.fileOpen = True

      def CloseFileConn(self):
            assert self.fileOpen, "File is not open"
            self.fileConn.Close()

      def LoadData(self):
            assert self.fileOpen, "File is not open"
            self.fileData = self.ReadData()

      def ReadData(self,items="All"):
            assert isinstance(items,(str,list,tuple)), "items must be of either types string, list, tuple"
            assert self.fileOpen, "File must first be opened"
            assert self.fileConn.FileInfo.TimeAxis.get_TimeUnit() == 1400, "Program hardcoded to seconds. TimeUnit code in file: %s" % self.fileData.FileInfo.TimeAxis.get_TimeUnit()

            times = list()
            
            startTimeSys = self.fileConn.FileInfo.TimeAxis.get_StartDateTime()
            startTime = dt.datetime(year=startTimeSys.Year,month=startTimeSys.Month,day=startTimeSys.Day,hour=startTimeSys.Hour,minute=startTimeSys.Minute,second=startTimeSys.Second,microsecond=startTimeSys.Millisecond) + dt.timedelta(seconds=self.fileConn.FileInfo.TimeAxis.get_StartTimeOffset())
            counter=0

            self.xCount = self.fileConn.ItemInfo[0].SpatialAxis.XCount
            self.yCount = self.fileConn.ItemInfo[0].SpatialAxis.YCount
            if self.fileConn.ItemInfo[0].SpatialAxis.Dimension == 3:
                  self.zCount = self.fileConn.ItemInfo[0].SpatialAxis.zCount
                  outputData = np.empty((self.yCount,self.xCount,self.zCount,0))
            else:
                  self.zCount = 0
                  outputData = np.empty((self.yCount,self.xCount,1,0))
                  
            for i in range(1,self.fileConn.ItemInfo.Count+1):
                  itemInfo = self.fileConn.ItemInfo.get_Item(i-1)
                  itemName = itemInfo.get_Name()
                  if itemName in items or items == "All":

                        metaData = {}
                        quantity = itemInfo.get_Quantity()
                        metaData["Type"] = quantity.get_ItemDescription()
                        metaData["Unit"] = quantity.get_UnitAbbreviation()
                        metaData["UnitNum"] = quantity.get_UnitAbbreviation().split(sep="/")[0]
                        metaData["UnitDenom"] = quantity.get_UnitAbbreviation().split(sep="/")[1]
                        metaData["UnitInt"] = quantity.get_UnitInt()
                        metaData["Name"] = itemName
                        metaData["ValueType"] = itemInfo.get_ValueType()
                        metaData["ItemInt"] = quantity.get_ItemInt()

                        for t in range(self.fileConn.FileInfo.TimeAxis.get_NumberOfTimeSteps()):
                              dataSys = self.fileConn.ReadItemTimeStep(i,t)
                              
                              if self.zCount == 0:
                                    data = np.array(list(dataSys.get_Data())).reshape((self.yCount,self.xCount,1,1))
                                    outputData = np.append(outputData,data,axis=3)
                              else:
                                    data = np.array(list(dataSys.get_Data())).reshape((self.yCount,self.xCount,self.zCount,1))
                                    outputData = np.append(outputData,data,axis=3)
                        
                              if i == 1:
                                    times = startTime + dt.timedelta(seconds=int(dataSys.get_Time()))

            return (times,outputData,metaData)

if __name__ == '__main__':
      pass
