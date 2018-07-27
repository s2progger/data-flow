# Data Flow ETL

A tool that can be used to move data from one datasource to another. Initially only JDBC compatible databases will be 
supported, but eventually additional sources such as web services could be added.


## Building

This project uses Gradle, so please refer to the Gradle documentation if you are unfamiliar with this build tool. Also
included in the repository is a [Jetbrains IDEA](https://www.jetbrains.com/idea/) project that can be opened in 
the free Community Edition. This is for my own continence and isn't necessary for editing or building the codebase.

To create a standalone executable application, use the following Gradle task:

``createExe``

### Build prerequisite

The standalone executable will bundle a copy of the JRE, but in order for this to work you must have your *JAVA_HOME* 
environment variable set. An example of how this might be set on Windows can be seen below:

``JAVA_HOME=C:\Program Files\Java\jdk1.8.0_181\jre``

At the moment only Windows has been tested, but in theory this will work on any platform.