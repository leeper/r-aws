
##' ##' AWS Support Function: set up credentials
##'
##' sets up the credentials needed to access AWS and optionally sets environment
##' variables for auto loading of credentials in the future
##' @param awsAccessKeyText your AWS Access Key as a string
##' @param awsSecretKeyText your AWS Secret Key as a string
##' @param setEnvironmentVariables T/F would you like environment variables to be set so
##' Segue will read the credentials on load
##' @author James "JD" Long
##' @export
setCredentials <- function(awsAccessKeyText, awsSecretKeyText, setEnvironmentVariables = TRUE){
    awsCreds <- new(com.amazonaws.auth.BasicAWSCredentials, awsAccessKeyText, awsSecretKeyText)
    assign("awsCreds", awsCreds, envir = .GlobalEnv)

    if (setEnvironmentVariables == TRUE) {
      Sys.setenv(AWSACCESSKEY = awsAccessKeyText, AWSSECRETKEY = awsSecretKeyText)
    }
}

##' AWS Support Function: Delete an S3 Key (a.k.a file)
##'
##' Deteles a key in a given bucket on S3
##' @param bucketName name of the bucket
##' @param keyName the key in the bucket
##' @author James "JD" Long
##' @export
deleteS3Key <- function(bucketName, keyName){
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()
  if (s3$doesBucketExist(bucketName)) { 
    s3$deleteObject(bucketName, keyName)
  }
}

##' AWS Support Function: Empty an S3 bucket
##'
##' Deletes all keys in the designated bucket
##' @param bucketName Name of the bucket to be emptied
##' @author James "JD" Long
##' @export
emptyS3Bucket <- function(bucketName){
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()

  # TODO: need a check to make sure the current user owns the bucket
  #       before trying to delete everything in it
  #       there's some risk this might loop forever if they don't own the bucket
  if (s3$doesBucketExist(bucketName)) {  
    lst <- s3$listObjects(bucketName)
    objSums <- lst$getObjectSummaries()
    listJavaObjs <- .jevalArray(objSums$toArray())
    if (length(listJavaObjs)>0){
      for (i in 1:length(listJavaObjs)) {
        deleteS3Key(bucketName, listJavaObjs[[i]]$getKey()[[1]])
      }
    }
    if (lst$isTruncated()){
      emptyS3Bucket(bucketName)
    }
  }
}

##' AWS Support Function: Delete an S3 Bucket
##'
##' Does nothing if the bucketName does not exist. If bucket contains Keys, all keys are deleted.
##' @param bucketName the bucket to be deleted
##' @author James "JD" Long
##' @export
deleteS3Bucket <- function(bucketName){
  tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
  s3 <- tx$getAmazonS3Client()
  if (s3$doesBucketExist(bucketName) == TRUE) {
    emptyS3Bucket(bucketName)
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    s3$deleteBucket(bucketName)
  }
}

##' AWS Support Function: Creates an S3 Bucket
##'
##' Creates an S3 bucket. If the bucket already exists, no warning is returned.
##' @param bucketName string of the name of the bucket to be created
##' @author James "JD" Long
##' @export
makeS3Bucket <- function(bucketName){
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    #test if the bucket exists; if not,  make bucket
    if (s3$doesBucketExist(bucketName) == FALSE) {
      s3$createBucket(bucketName)
    } else {
      warning("Unable to Create Bucket", call. = FALSE)
    }
}

##' AWS Support Function: Uploads a local file to an S3 Bucket
##'
##' If buckName does not exist, it is created and a warning is issued. 
##' @param bucketName destination bucket
##' @param localFile local file to be uploaded
##' @author James "JD" Long
##' @export
uploadS3File <- function(bucketName, localFile){
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    fileToUpload <-  new(File, localFile)
    request <- new(com.amazonaws.services.s3.model.PutObjectRequest, bucketName, fileToUpload$getName(), fileToUpload)
    s3$putObject(request)
}

##' AWS Support Function: Downloads a key from an S3 Bucket into a local file.
##'
##' Pulls a key (file) from a bucket into a localFile. If the keyName = ".all" then
##' all files from the bucket are pulled and localFile should be a
##' directory name. Ignores "sub directories" in buckets. 
##' @param bucketName destination bucket
##' @param keyName key to download. ".all" to pull all keys
##' @param localFile local file name or path if ".all" is called for keyName
##' @author James "JD" Long
##' @export
downloadS3File <- function(bucketName, keyName, localFile){
    tx <- new(com.amazonaws.services.s3.transfer.TransferManager, awsCreds)
    s3 <- tx$getAmazonS3Client()
    if (keyName != ".all") {
      request <- new(com.amazonaws.services.s3.model.GetObjectRequest, bucketName, keyName)
      theObject <- s3$getObject(request, new(java.io.File, localFile))
    } else {
     # this will only pull the first page of listings
     # so if there are a lot of files it won't grab them all
     # 
     # TODO: make it pull multiple pages of files
     # TODO: pull subdirectories too
      system(paste("mkdir", localFile), ignore.stderr = TRUE)
      lst <- s3$listObjects(bucketName)
      objSums <- lst$getObjectSummaries()
      listJavaObjs <- .jevalArray(objSums$toArray())
      if (length(listJavaObjs)>0){
        for (i in 1:length(listJavaObjs)) {
          # if statement here just to filter out subdirs
          key <- listJavaObjs[[i]]$getKey()[[1]]
          if ( length( unlist(strsplit(key, split="/")) ) == 1) {
            if (substring( key, nchar( key ) - 7, nchar( key ) )  != "$folder$") {
              localFullFile <- paste(localFile, "/", listJavaObjs[[i]]$getKey()[[1]], sep="")
              downloadS3File(bucketName, listJavaObjs[[i]]$getKey()[[1]], localFullFile)
            }
          }
        }
      }
    }
  }
  
