
##' @import rJava
.onLoad <- function(lib, pkg) {
    pathToSdk <- paste(system.file(package = "r-aws") , "/aws-java-sdk/", sep="")

    jarPaths <- c(paste(pathToSdk, "lib/aws-java-sdk-1.1.0.jar", sep=""),
                  paste(pathToSdk, "third-party/commons-logging-1.1.1/commons-logging-1.1.1.jar", sep=""),
                  paste(pathToSdk, "third-party/commons-httpclient-3.0.1/commons-httpclient-3.0.1.jar", sep=""),
                  paste(pathToSdk, "third-party/commons-codec-1.3/commons-codec-1.3.jar", sep=""),
                  paste(pathToSdk, "third-party/log4j-1.2.16.jar", sep=""),
                  paste(pathToSdk, "third-party/", sep="")
                  )
    .jpackage(pkg, morePaths=jarPaths)
    attach( javaImport( c("java.lang", "java.io")))
    
    if (Sys.getenv("AWSACCESSKEY") != "" && Sys.getenv("AWSSECRETKEY") != ""){
      awsCreds <- new(com.amazonaws.auth.BasicAWSCredentials, Sys.getenv("AWSACCESSKEY"), Sys.getenv("AWSSECRETKEY"))
      assign("awsCreds", awsCreds, envir = .GlobalEnv)
      packageStartupMessage( "Segue has loaded your AWS Credentials." )
    } else {
       packageStartupMessage( "Segue did not find your AWS credentials. Please run the setCredentials() function." )
    }
}
