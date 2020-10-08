create database db_webscience


create table Research(
R_ID int identity primary key,
Title varchar(Max),
Journal varchar(Max),
Research_Language varchar(Max),
DocType varchar(Max),
Abstraction varchar(max),
No_of_references int,
Time_cited int,
Last_180_days_down int,
Since_2013 int,
Published_year int,
Start_page varchar(max),
End_page varchar(max),
)


create table Authors(
Author_ID int identity primary key,
Author_Name varchar(max),
Institute_Name varchar(max),
Department_Name varchar(max),
Source_author varchar (max),
)


create table Reprint_Author(
Research_id int foreign key references Research(R_ID),
author_name varchar(max),
Institute_name varchar(max),
Department_Name varchar(max),
Email_ID varchar(max),
)


create table Web_of_science(
Research_id int foreign key references Research(R_ID),
Web_ofscience varchar(max),
)


create table Research_Area(
Research_id int foreign key references Research(R_ID),
Research_Area varchar(max),
)


create table Research_Authors(
Research_ID int foreign key references Research(R_ID),
Author_ID int foreign key references Authors(Author_ID),
)


create table Authors_Keywords(
Research_ID int foreign key references Research(R_ID),
AK_Name varchar(max),
)


create table Keywords_Plus(
Research_ID int foreign key references Research(R_ID),
KP_Name varchar(max),
)

	












