-- Exploratory Data Analysis

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

-- normally when you start the EDA process you have some idea of what you're looking for

-- with this info we are just going to look around and see what we find!


SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2; 
# 12000 LAID OFF IN ONE GO and in percentage_laid_off 1 represents 100% percentage_laid_off

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;
-- these are mostly startups it looks like who all went out of business during this time
-- if we order by funcs_raised_millions we can see how big some of these companies were

-- Companies with the most Total Layoffs

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

-- by location

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;

-- this it total in the past 3 years or in the dataset

SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;


SELECT stage , SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;

-- Looking at Percentage to see how big these layoffs were

SELECT company , SUM(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT company , AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;


-- Rolling Total of Layoffs Per Month
-- group this as 
SELECT substring(`date`,1,7) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC;

-- now use it in a CTE so we can query off of it
WITH rolling_total AS
(
SELECT substring(`date`,1,7) AS `MONTH`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
WHERE substring(`date`,1,7) IS NOT NULL
GROUP BY `MONTH`
ORDER BY 1 ASC
)
SELECT `MONTH`, total_laid_off, SUM(total_laid_off) OVER(ORDER BY `MONTH`) AS rolling_total
FROM rolling_total;

-- Extracts the year and month from the date column in the format YYYY-MM. This effectively groups the data by month.
-- SUM(total_laid_off) AS total_laid_off: Calculates the total layoffs for each month.
-- WHERE substring(date,1,7) IS NOT NULL: Ensures that only non-null dates are considered.
-- GROUP BY MONTH: Groups the data by the extracted month.
-- ORDER BY 1 ASC: Orders the results by the month in ascending order.


-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year.

SELECT company ,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC;


WITH company_Year(company, years, total_laid_off) AS
(
SELECT company ,YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT * ,
DENSE_RANK() OVER(PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_Year
WHERE years IS NOT NULL
)
SELECT * 
FROM company_Year_Rank  # filter on rank
WHERE Ranking <=5;
-- WANT TO FILTER RANKING TO BE ABLE TO FILTER TOP 5 COMPANIES PER YEAR

SELECT *
FROM layoffs_staging2;