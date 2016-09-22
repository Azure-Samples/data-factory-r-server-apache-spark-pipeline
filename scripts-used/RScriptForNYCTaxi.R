# accept the arguments from command line
args = (commandArgs(TRUE))

if (length(args) < 2) {
    stop ("At least 2 argument must be supplied. 1st for the location of the 
        dataset. 2nd is the directory on hdfs for the output model storage", call.=FALSE) 
}

# print the arguments for debugging purposes
# Set the spark context and define the executor memory and processor cores. 



  outputFile <- args[2]

  inputFile  <- args[1]
  #name of the final model object
  tempLocalModelFile <- 'model'
  filePrefix <- "/tmp/"
  rxSetComputeContext(RxSpark(hdfsShareDir = "/tmp", consoleOutput=TRUE, executorMem="2g", executorCores = 2, driverMem="1g", executorOverheadMem="1g") )
  hdfsFS <- RxHdfsFileSystem()

  


xdfOutFile       <- file.path(filePrefix, "nyctaxixdf")
taxiSplitXdfFile <- file.path(filePrefix, "taxiSplitXdf")
taxiTrainXdfFile <- file.path(filePrefix, "taxiTrainXdf")
taxiTestXdfFile  <- file.path(filePrefix, "taxiTestXdf")
predictionFile   <- file.path(filePrefix, "predictedRF")



varsToDrop = c("medallion", "hack_license","store_and_fwd_flag",
               "pickup_datetime", "rate_code",
               "dropoff_datetime","pickup_longitude",
               "pickup_latitude", "dropoff_longitude",
               "dropoff_latitude ", "surcharge",
               "mta_tax", "tolls_amount", "total_amount")



taxiColClasses <- list(medallion = "character", hack_license = "character",
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



colInfo <- list()

for (name in names(taxiColClasses))
  colInfo[[paste("V", length(colInfo)+1, sep = "")]] <- list(type = taxiColClasses[[name]], newName = name)



taxiDS <- RxTextData(file = inputFile, fileSystem = hdfsFS, delimiter = ",", firstRowIsColNames = FALSE,
                     colInfo = colInfo)



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
model <-  rxLinMod(tip_amount ~ fare_amount + vendor_id + pickup_hour + pickup_week + 
                    pickup_weekday + passenger_count  + trip_time_in_secs + trip_distance + 
                    payment_type,data =taxiSplitXdf)

pt2 <- proc.time()
runtime <- pt2-pt1; 
print (runtime/60)

save(model, file=tempLocalModelFile)
rxHadoopCopyFromLocal(tempLocalModelFile,outputFile)

