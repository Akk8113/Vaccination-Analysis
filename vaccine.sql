create database vaccination

use vaccination

create table vaccine_introduction(
ISO_3_code varchar(255), Countryname varchar(255),
Who_region varchar(255), Year int, Description varchar(255)
)


ALTER TABLE vaccine_introduction
ADD Intro varchar(50)

create table vaccine_schedule(
ISO_3_code varchar(255), Countryname varchar(255),
Who_region varchar(255), Year int, Vaccinecode varchar(255), 
Vaccince_description varchar(255), Schedulerounds int,
Targetpop varchar(255), Target_Description varchar(255),
Geoarea varchar(255), Ageadministered varchar(255), Sourcecomment varchar(255)
)

CREATE TABLE CoverageData (
    [Groupn] NVARCHAR(255),
    [Code] NVARCHAR(50),
    [Name] NVARCHAR(255),
    [Year] INT,
    [Antigen] NVARCHAR(100),
    [Antigen_description] NVARCHAR(255),
    [Coverage_category] NVARCHAR(100),
    [Coverage_category_description] NVARCHAR(255),
    [Target_number] FLOAT,
    [Doses] FLOAT,
    [Coverage] FLOAT
);


drop table coverage_date

EXEC sp_rename 'TableName.OldColumnName', 'NewColumnName', 'COLUMN';
EXEC sp_rename 'coverage_date.coverage_category_description','Coverage_category_description','COLUMN'
EXEC sp_rename 'vaccine_schedule.vaccince_description','Vaccine_description','COLUMN'



create table reported_cases_data(
Groupn varchar(255), Code varchar(255), Name varchar(255),
Year int,Disease varchar(255), Disease_description varchar(255), Cases int)

create table incidence_rate_date(Groupn varchar(255), Code varchar(255), Name varchar(255),
Year int ,Disease varchar(255), Disease_description varchar(255), Denominator varchar(255),
Incidence_rate float
)

Select * from incidence_rate_date
select * from Coverage_date
Select * from reported_cases_data
Select * from vaccine_introduction
Select * from vaccine_schedule

drop table CoverageData
drop table reported_cases_data