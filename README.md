# Table2Pojo

Simple tool to generate POJO classes from database tables/columns. 

Supports all JDBC supported databases. Includes following JDBC drivers: 
- Oracle 
- MySQL 
- PostgreSQL 

JDBC drivers can be added/updated in `build.gradle` file. 

## Build

Use gradle to build and assemble jar files with dependencies. 
```bash
gradle fatJar
```

## Run

Configure the database connection properties in `db.properties` file in current directory: 

```
driver=oracle.jdbc.OracleDriver
url=jdbc:oracle:thin:@localhost:1521:orcl
username=dbusername
password=dbpassword
```

Run the jar with arguments. 
```bash
java -jar build/libs/table2pojo-all.jar <options>
```

## Options 

Option | Description
-------|------------
h | help 
a | generate POJOs for all the tables in database  
t | list of database tables delimited by ; (semicolon). overrides `a` option 
p | (optional) java package name of the POJOs. If not specified, default/blank package will be used. 
d | (optional) target directory where POJOs (.Java files) are generated. If not specified, current directory will be used 
