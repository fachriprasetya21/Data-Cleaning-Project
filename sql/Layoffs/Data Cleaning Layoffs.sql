SELECT * FROM layoffs;

-- Step on data cleaning
-- 1. Remove duplicates data
-- 2. Standarized the data
-- 3. Remove Null and blank values
-- 4. Remove any columns that didnt needed

-- 1. Remove duplicates data
-- before you change the original data you must be warry the data being lost. So you must add another table with origin data

CREATE TABLE layoffs_staging
LIKE layoffs;

-- add the data
INSERT layoffs_staging
SELECT * FROM layoffs;

-- check if it works
SELECT * FROM layoffs_staging;

-- Check if have any duplicates
SELECT * ,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off,
 'date', stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicate_cte AS (
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

-- (double check data)
SELECT *
FROM layoffs_staging
WHERE company = 'Service';


-- we cannot just delete the duplicate, but we must do create another table then the new table gonna have updated with no duplicate

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
SELECT * ,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;



-- 2. Standarized the data

SELECT DISTINCT company
FROM layoffs_staging2 
ORDER BY 1;

UPDATE layoffs_staging2
SET company = TRIM(company);


SELECT DISTINCT industry            -- in this code was founded (crypto,cryptocurrency,crypto currency) so we need to made it one kind
FROM layoffs_staging2 
ORDER BY 1;

SELECT *                           -- Check how much the data have this difference
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Update the the new industry 
UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT industry 
FROM layoffs_staging2 
ORDER BY 1;

-- Check everything to the others columns if there any suspicious or different data

SELECT DISTINCT location 
FROM layoffs_staging2 
ORDER BY 1;

SELECT DISTINCT country 
FROM layoffs_staging2            -- In the country columns have minor diff (Have 2 USA, 1 has "." in the end of it
ORDER BY 1;

SELECT *
FROM layoffs_staging2 
WHERE country LIKE 'United States%'
ORDER BY 1;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2 
WHERE country LIKE 'United States%'
ORDER BY 1;

UPDATE layoffs_staging2             -- Update the coloumn country
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- change the format of date column from str to date

SELECT date,
str_to_date(date, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET date = str_to_date(date, '%m/%d/%Y');

SELECT date
FROM layoffs_staging2;

-- Change date data type
ALTER TABLE layoffs_staging2
MODIFY COLUMN date DATE;


-- 3. Null and Blank Data

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_staging2 -- Check if there any same on industry so the blank or Null gonna be like the other that has industry if the company and location is the same
WHERE company = 'Airbnb';


SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;


UPDATE  layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging2   -- Check if there any industry Null left, if there is no another data then let it be
WHERE industry IS NULL
OR industry = '';


SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL -- Cant trust the data because didnt have more information about it (1. Find resource more, 2. Delete it so the data will more accurate)
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2      -- drop the column of the row_num
DROP COLUMN row_num;


-- All set!! The data is now can be use 