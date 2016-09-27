# accept the arguments from command line
args = (commandArgs(TRUE))

if (length(args) < 3) {
    stop ("At least 3 argument must be supplied. 1st for the location of the 
        dataset. 2nd is the location of the model file on hdfs. 3rd is directory on hdfs for the output model storage", call.=TRUE) 
}

# print the arguments for debugging purposes
# Set the spark context and define the executor memory and processor cores. 


  #location for the prediction output file in the HDFS
  outputFile <- args[3]

  #Location of the model file

  modelFile <- args[2]

  #dataset for testing
  inputFile  <- args[1]
  #name of the final model object

  
  filePrefix <- "/tmp/"
  localFolderForTestingModel <-'testingModel'
  rxSetComputeContext(RxSpark(hdfsShareDir = "/tmp", consoleOutput=TRUE, executorMem="2g", executorCores = 2, driverMem="1g", executorOverheadMem="1g") )
  hdfsFS <- RxHdfsFileSystem()
  
  #extract the filename
  strsplit(modelFile,'/')->modelPathSplit
  modelFileName <-  modelPathSplit[[1]] [length(modelPathSplit[[1]])]

  #check if the model file exists on hdfs 
  if (rxHadoopFileExists(modelFile) == FALSE) {
    stop (paste("Model file not found at location: ", modelFile, sep = ' ') , call.=TRUE) 
  }

  localModelFileWithPath =  paste(localFolderForTestingModel,modelFileName, sep = "/")

  #copy model file locally after removing the existing file 
  if (file.exists(localModelFileWithPath)==TRUE) {
      file.remove(localModelFileWithPath)
  }
  
  #create local folder (this gets ignored if folder exists)
dir.create(localFolderForTestingModel, showWarnings = FALSE)

modelFile
localModelFileWithPath
# copy the model to this location
rxHadoopCopyToLocal(modelFile,localModelFileWithPath)

# load the model into an object named - model 
load(localModelFileWithPath)

xdfOutFile       <- file.path(filePrefix, "nyctaxixdf")
taxiSplitXdfFile <- file.path(filePrefix, "taxiSplitXdf")
taxiTrainXdfFile <- file.path(filePrefix, "taxiTrainXdf")
taxiTestXdfFile  <- file.path(filePrefix, "taxiTestXdf")
stackSplitXdf <- RxXdfData(file = outputFile, fileSystem = hdfsFS);


varsToDrop = c("medallion", "hack_license","store_and_fwd_flag",
               "pickup_datetime", "rate_code",
               "dropoff_datetime","pickup_longitude",
               "pickup_latitude", "dropoff_longitude",
               "dropoff_latitude ", "surcharge",
               "mta_tax", "tolls_amount", "total_amount")



taxiColClasses <- c(medallion = "character", hack_license = "character",
                       vendor_id =  "factor", rate_code = "factor",
                       store_and_fwd_flag = "character", dropoff_datetime = "character",
                       pickup_datetime = "character", pickup_hour = "numeric",
                       pickup_week = "numeric", pickup_weekday = "numeric",
                       passenger_count = "numeric", trip_time_in_secs = "numeric",
                       trip_distance = "numeric", pickup_longitude = "numeric",
                       pickup_latitude = "numeric", dropoff_longitude = "numeric",
                       dropoff_latitude = "numeric",
                       payment_type = "factor", fare_amount = "numeric",
                       surcharge = "numeric", mta_tax = "numeric", tip_amount = "numeric",
                       tolls_amount = "numeric", total_amount = "numeric")




taxiDS <- RxTextData(file = inputFile, fileSystem = hdfsFS, delimiter = ",", firstRowIsColNames = TRUE,
                     colClasses = taxiColClasses)



xdfOut <- RxXdfData(file = xdfOutFile, fileSystem = hdfsFS)



taxiDSXdf <- rxImport(inData = taxiDS, outFile = xdfOut,
                      createCompositeSet = TRUE,
                      overwrite = TRUE)






taxiSplitXdf <- RxXdfData(file = taxiSplitXdfFile, fileSystem = hdfsFS);

rxDataStep(inData = taxiDSXdf, outFile = taxiSplitXdf,
           varsToDrop = varsToDrop,
           rowSelection = (passenger_count > 0 & passenger_count < 8 &
                             tip_amount >= 0 & tip_amount <= 40 &
                             fare_amount > 0 & fare_amount <= 200 &
                             trip_distance > 0 & trip_distance <= 100 &
                             trip_time_in_secs > 10 & trip_time_in_secs <= 7200),
           overwrite = TRUE)




            
# run the training model

pt1 <- proc.time()
#rxPredict(model, data =taxiSplitXdf, outData=stackSplitXdf, checkFactorLevels=FALSE )
rxPredict(model, data =taxiSplitXdf,checkFactorLevels=FALSE )

pt2 <- proc.time()
runtime <- pt2-pt1; 
print (runtime/60)

