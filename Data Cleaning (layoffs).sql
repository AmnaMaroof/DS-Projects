SELECT *
FROM layoffs;


# Requirements 
-- 1. Remove duplicates
-- 2. Standarize Data
-- 3. NULL Values or blank values 
-- 4. Remove columns which are not necasssary 

-- Data_staging: It serves as a temporary space to hold raw data extracted from various sources before it undergoes data transformation 
-- and is loaded into the final data or data marts.

Create table layoffs_staging 
LIKE layoffs;

Insert layoffs_staging
Select *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Removing Duplicates
# In general every data has a unique id but in this there is no unique id, so what we can do is row number and try to match
# each columnn then we will see  any duplicates 

-- Select *,
-- Row_Number() Over 
-- (partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) As row_num
-- FROM layoffs_staging;


-- Now to filter the row_num to check if there is duplicates in the column, we can use CTE 

With CTE_Duplicates AS 
(
Select *,
Row_Number() Over 
(partition by company, location, industry, total_laid_off, percentage_laid_off, 
'date', stage, country, funds_raised_millions) As row_num
FROM layoffs_staging
)
Select *
FROM CTE_Duplicates
WHERE row_num > 1;

-- now this shows the duplicates 
SELECT *
FROM layoffs_staging
WHERE company = 'casper';

-- Duplicates in a database refer to multiple rows or records that have identical values in one or more columns, leading to redundancy 
-- and potential data integrity issues. These duplicates can occur due to various reasons such as data entry errors, merging of multiple datasets, 
-- or lack of constraints in the database design.
CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2;

INSERT INTO layoffs_staging2
Select *,
Row_Number() Over 
(partition by company, location, industry, total_laid_off, percentage_laid_off, 
'date', stage, country, funds_raised_millions) As row_num
FROM layoffs_staging;

SET SQL_SAFE_UPDATES = 0;

DELETE 
FROM layoffs_staging2
WHERE row_num > 1;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- 2. Standarize Data (finding the issues in the data set and fixing it.)


SELECT company,TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = (TRIM(company)); # TO remove the white spacing 

SELECT distinct industry 
FROM layoffs_staging2 
ORDER BY 1; 

-- NOW if we look into the  matter crypto, crypto currency these are all the same. the reason we need to change
-- them is beacsue when we do the explolatoray analysis these will all have their own rows and columns which we don't want we want
-- we want them to be grouped by one

SELECT *
FROM layoffs_staging2 
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT distinct location
FROM layoffs_staging2
ORDER BY 1;

-- apparently we have some "United States" and some "United States." with a period at the end. Let's standardize this.

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'united states%'
ORDER BY 1;


SELECT distinct country ,TRIM(trailing '.'  from country)
FROM layoffs_staging2
ORDER BY 1; 

# The TRIM(TRAILING '.' FROM country) part of the query will remove any trailing underscores from the values in the country column

UPDATE layoffs_staging2
SET company = TRIM(trailing '.'  from country)
WHERE company LIKE 'united states%';


SELECT `date`
FROM layoffs_staging2; 
-- for EDA we want to chnage this date format from text to date and time.

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y') #  (with a 4-digit year)date
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE; # change the data type from text to date 

SELECT *
FROM layoffs_staging2;


-- 3. NULL Values or blank values 

SELECT *
FROM layoffs_staging2 
WHERE percentage_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';


UPDATE layoffs_staging2
SET industry = null
WHERE industry LIKE '';

SELECT *
FROM layoffs_staging2
WHERE company LIKE 'Airbnb'; 

-- it looks like airbnb is a travel, but this one just isn't populated.
-- I'm sure it's the same for the others. What we can do is
-- write a query that if there is another row with the same company name, it will update it to the non-null industry values
-- makes it easy so if there were thousands we wouldn't have to manually check them all

SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company = t2.company
    AND t1.location = t2.location 
    where t1.industry IS NULL
    AND t2.industry IS NOT NULL;
    
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company = t2.company
SET t1.industry = t2.industry
where t1.industry IS NULL
AND t2.industry IS NOT NULL;


-- the null values in total_laid_off, percentage_laid_off, and funds_raised_millions all look normal


-- 4. Remove columns which are not necasssary 

SELECT *
FROM layoffs_staging2 
WHERE percentage_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2 
WHERE percentage_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP column row_num;

SELECT *
FROM layoffs_staging2;