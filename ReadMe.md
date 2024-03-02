<p align="center"> 
    <img height=200 src="logo.png"/> 
</p>
<h1> <div align="center">Data Cleaners</div> </h1>

<div align="center">

[![Contributors][contributors-shield]][contributors-url]
[![Commits][commits-shield]][commits-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![MIT License][license-shield]][license-url]

#### A Java application that applies user-defined rules to datasets, with the goal of enforcing data quality.
</div></h4>

Data Cleaning is an important precursor to any form of data analysis. Data Cleaners is a Spark-based Application with the responsibility of cleaning any datasets provided, either from files or database tables. A user may define a set of rules to construct a request, which in return will produce a detailed report of each entry that violates any of the rules. Violated entries can then be seperated or removed from the original dataset, thus ensuring data quality.
<hr style="width:100%;text-align:left;margin-left:0">  


## Features


* Registration of Datasets from both CSV files and database tables, without relying on memory.

* Construction and execution of robust requests, on registered datasets, by uttilizing a wide range of heavily customisable checks: 
  * Primary Key Check for column uniqueness.✅
  * Foreign Key Check between two registered datasets. ✅
  * Domain Type Check for column validity towards the data's type. ✅
  * Domain Value Check for checking if a column contains only certain values.✅
  * Format Check for column validity and consistency. ✅
  * Not Null Check for column completeness.✅
  * Numeric Constraint Check for making sure a column has numeric values within a defined range.✅
  * User Defined Expression Check for complex user-defined mathematical expression tests regarding a single entry.✅
  * User Defined Conditional Check for complex user-defined mathematical expression tests regarding single entries that follow a certain condition.✅
  * User Defined Aggregation Checks for complex user-defined mathematical expression tests using, aggregation functions, regarding a single entry.✅
  * User Defined Group Checks for complex user-defined mathematical expression tests regarding groups of entries.❌

* Detailed log generation for each executed request in several formats:
  * TXT File ✅
  * HTML File ❌
  * MARKDOWN File❌

* Violating Row Policy; Different options for handling rejected (or invalid) entries
  * WARN: Generate the log. ✅
  * ISOLATE: Generate the log, and produce two TSV files; one with the rejected and one with the passed entries. ✅
  * PURGE: Generate the log, and produce one TSV file with just the passed entries. ✅


## Set-Up


Choose the IDE of preference and import the project as a Maven Project. Make sure to set-up the JAVA_HOME property to the correct location of a Java 8 or above installation.

Afterwards, check the `Client` class for an example on the creation, execution and report production of a request.


## Tests


All tests are stored within the `test` folder. To execute all of them simply run:

```bash
./mvnw test
```
Since it's a Maven script, ensure that M2_HOME and MAVEN_HOME system variables have been set correctly.

## Usage

Consider that you need to perform some form of analysis on a dataset that follows this schema:
|ID|Name|Wage|
|----|------|----|
|1|John|120|
|2|Mike|null|
|3|Samantha|500|
|2|Jane|-1|
|5|Bob|102213|

This dataset however, not only is it large, but it also contains several quality issues. We first define some logical rules for this schema:

- The `ID` column contains unique, not null, numeric values.
- The `Name` column contains non numeric strings.
- The `Wage` column contains numeric values that should be between 0 and 1_000.

With the help of our application, we can now create a quality enforcing request and determine which entries are problematic. First, we need to register the dataset in quest.

```java
    FacadeFactory facadeFactory = new FacadeFactory();
    IDataCleanerFacade facade = facadeFactory.createDataCleanerFacade();

    boolean hasHeader = true;
    String frameName = "dataset";
    facade.registerDataset("path//of//file//dataset.csv", frameName, hasHeader);
```

With the dataset registered via our facade, we proceed to define our request:

```java

ClientRequest req = ClientRequest.builder()
                        .onDataset("dataset") //The name used during registration
                        //For the ID column
                        .withPrimaryKeys("ID")
                        .withColumnType("ID", DomainType.INTEGER)
                        //For the Name column
                        .withColumnType("Name", DomainType.ALPHA)
                        //For the Wage column
                        .withColumnType("Wage", DomainType.NUMERIC)
                        .withNumericColumn("Wage", 0, 1_000)
                        .withViolationPolicy(ViolatingRowPolicy.PURGE)
                        .build()
      
facade.executeClientRequest(req);
```

We also choose the PURGE violating row policy in order to immediatly dispose of all problematic entries when generating a report.

```Java
facade.generateReport("dataset", "output//path//directory", ReportType.TEXT);
```

Finally, we call the `generateReport` function to create a `log.txt` file, as well as a TSV file with all the conforming entries.


## Contributors

Nikolaos Taflampas <br>
Panos Vassiliadis

[contributors-shield]: https://img.shields.io/github/contributors/DAINTINESS-Group/DataCleaners
[commits-shield]: https://img.shields.io/github/last-commit/DAINTINESS-Group/DataCleaners
[forks-shield]: https://img.shields.io/github/forks/DAINTINESS-Group/DataCleaners
[stars-shield]: https://img.shields.io/github/stars/DAINTINESS-Group/DataCleaners
[issues-shield]: https://img.shields.io/github/issues/DAINTINESS-Group/DataCleaners
[license-shield]: https://img.shields.io/github/license/DAINTINESS-Group/DataCleaners

[contributors-url]: https://github.com/DAINTINESS-Group/DataCleaners/graphs/contributors
[commits-url]: https://github.com/DAINTINESS-Group/DataCleaners/commit/main
[forks-url]: https://github.com/DAINTINESS-Group/DataCleaners/network/members
[stars-url]: https://github.com/DAINTINESS-Group/DataCleaners/stargazers
[issues-url]: https://github.com/DAINTINESS-Group/DataCleaners/issues/
[license-url]: https://github.com/DAINTINESS-Group/DataCleaners/blob/main/LICENSE