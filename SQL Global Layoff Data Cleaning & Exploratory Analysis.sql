

-- ----------------------- This is a SQL data cleaning and exploratory analysis project on global company layoff data from 2020 March to 2023 March-----------------------------------

-- creating a staging table with the same columns as the original to work on
CREATE TABLE `staging` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- -------------------------------------------------------------------- Removing Duplicates --------------------------------------------------------------------------------------
-- Insert records from the original into the staging table
-- &
-- Adding a column using a window function to rank each unique record
--      * Duplicates would have a ranking of 2
insert into staging
select *,
row_number() over(
partition by 
company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num 
from layoffs;

-- Remove duplicate records
Delete
from staging
where row_num > 1;

-- -------------------------------------------------------------------- Standardizing data --------------------------------------------------------------------------------------------
-- Using wildcards to identify inconsistent records and updating them
-- converting 'date' formats and data types
-- Standardizing all blank/null values 
-- Using self-join to populate null values 
-- Removing irrelevant rows and columns

-- Standardizing Company Names
Update staging
set 
company = trim(company);

-- Standardizing Industry Names
Update staging
set
industry = 'Crypto'
where
industry like 'crypto%';

-- Standardizing Country Names
Update staging
set
country = 'United States'
where 
country like 'United States%';

-- converting the 'date' into a m/d/y format
Update staging
set
`date` = str_to_date(`date`, '%m/%d/%Y');

-- Updating 'date' column to 'date' data type
alter table staging
modify column `date` date;

-- Converting all blank industry records to null values
Update staging
set industry = null
where industry = '';

-- self join to populate null industries if the company is the same
update staging table1
join staging table2
on table1.company = table2.company
set table1.industry = table2.industry
where table1.industry is null
and table2.industry is not null;

-- Remove records where both total_laid_off and percentage_laid_off are null because they will not contribute to any meaningful insights
Delete 
from staging
where total_laid_off is null 
and
percentage_laid_off is null;

-- Removing the row_num column that was added to remove duplicates
alter table staging
drop column row_num;

-- -------------------------------------------------------------------- Exploratory Data Analysis --------------------------------------------------------------------------------------

-- Max layoffs and max % layoffs
select max(total_laid_off), max(percentage_laid_off)
from staging;

-- Identify the time frame of the data
select min(`date`), max(`date`)
from staging;

-- Sorting total layoffs per year in descending order
select year(`date`), sum(total_laid_off)
from staging
group by year(`date`)
order by year(`date`) desc;

-- Sorting total layoffs per business stage
select stage, sum(total_laid_off)
from staging
group by stage
order by sum(total_laid_off) desc;

-- Overview of data where percentage layoff is 100% by funds raised
select *
from staging
where percentage_laid_off = 1
order by funds_raised_millions desc;

-- Overview of data where percentage lay off is 100% by number of lay off
select *
from staging
where percentage_laid_off = 1
order by total_laid_off desc;

-- Industries with the most number of companies with 100% layoffs
-- * Retail, Food, and Finance were top 3
select industry, count(distinct company)
from staging
where percentage_laid_off = 1
group by industry
order by count(company) desc;

-- Companies with the most layoffs  
-- * Amazon, Google, Meta, Salesforce, Microsoft and Philips has layoffs >= 10,000
select company, sum(total_laid_off)
from staging
group by company
order by sum(total_laid_off) desc;

-- Industries with the most layoffs and the number of recorded companies
-- * Consumer, Retail, and Transportation had the most total layoffs.
-- * Finance ranked 4th but had 2x the number of comapnies on record.
select industry, sum(total_laid_off), count(distinct company)
from staging
group by industry
order by sum(total_laid_off) desc;

-- Industries with the highest % layoffs
-- * Aerospace had the highest % layoffs of 57% followed by Education at 36%.
select round(avg(percentage_laid_off),2) as avg_perc, industry
from staging
group by industry
order by avg_perc desc;

-- Countries with the most layoffs and recorded companies
-- * almost 90% of records were companies from the U.S 
select country, sum(total_laid_off), count( distinct company)
from staging
group by country
order by sum(total_laid_off) desc;

-- Ranking the top 5 companies with the most total layoffs for each year (2020-2023)
with ranked_companies as (
select company, year(`date`) as years, SUM(total_laid_off) as laid_off_total,
dense_rank() over (partition by year(`date`) order by SUM(total_laid_off) desc) as ranking
from staging
where year(`date`) is not null
group by company, years

)
select *
from ranked_companies
where ranking <= 5
order by years, ranking;
 
 -- Countries with the highest % of layoffs, and company count
 -- * Australia had the highest % layoff 39% with a meaningful sample size (>50 records)
select country, round(avg(percentage_laid_off),2), count(distinct company)
from staging
group by country
order by count(distinct company) desc;

-- Rolling total of monthly layoffs using a window function where the date is valid
with monthly_laid_off as 
(
select year(`date`) as `year`, month(`date`) as `month`, sum(total_laid_off) as laid_off
from staging
where `date` is not null
group by `year`, `month`
order by `year`, `month`
)
select `year`, `month`, laid_off, sum(laid_off) over (order by `year`, `month`) as rolling_total
from monthly_laid_off;

-- Same rolling total but as a subquery
select `year`, `month`, laid_off, sum(laid_off) over (order by `year`, `month`) as rolling_total
from 
(select year(`date`) as `year`, month(`date`) as `month`, sum(total_laid_off) as laid_off
from staging
where `date` is not null
group by `year`, `month`
order by `year`, `month`) as monthly_laid_off;

-- ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

select *
from staging
