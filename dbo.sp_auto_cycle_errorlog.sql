USE [master]
GO

IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME = 'sp_auto_cycle_errorlog')
	EXEC ('CREATE PROC [dbo].[sp_auto_cycle_errorlog] AS SELECT ''placeholder''')
GO

ALTER PROCEDURE [dbo].[sp_auto_cycle_errorlog] (
	 @ERRORLOG_max_size_bytes INT = 104857600 --100MB
	,@ERRORLOG_max_age_days INT = 7
) AS
	SET NOCOUNT ON;
  /*============================================================================================================ 
  Marcin Gminski; https://github.com/marcingminski
  --------------------------------------------------------------------------------------------------------------
  The most common approach to cycle ERRORLOG is to schedule an agent job to execute sp_cycle_errorlog. 
  This brings few challenges in very large enviroments that have both very busy and not so much busy servers. 
  With Both Failed and Successful logins enabled this escalates even further. A large (over 100MB) ERRORLOG 
  can take few minutes to open which impact monitoring tools (or us humans) that read it. I faced this 
  challenge long time ago and instead of tweaking how often the sp_cycle_errorlog should run on each out many 
  thousands servers I wrote this simple wrapper than checks the age and size of the existing ERRORLOG and 
  only recycles when one of these two is breached. Although I have used this in production for years, 
  as usual, please test in your enviroment first.
  --------------------------------------------------------------------------------------------------------------
  paremeters:
    @ERRORLOG_max_size_bytes INT  - file size in bytes, when ERRORLOG is bigger than that it will be cycled
    @ERRORLOG_max_age_days INT    - file age in days, when ERRORLOG is older than that it will be cycled
  --------------------------------------------------------------------------------------------------------------
  Schedule this daily and set size and age to your liking or leave the defaults of 100MB and 7 days 
  ============================================================================================================*/
  
	DECLARE	@return_value INT
	DECLARE	@output VARCHAR(50)
	DECLARE @table_error_logs TABLE (
			log_number tinyint
		,	log_date DATETIME
		,	log_bytes int)
	
	DECLARE @message VARCHAR(4000)
	DECLARE @ERRORLOG_size_formatted VARCHAR(20)
	DECLARE @ERRORLOG_max_size_formatted VARCHAR(20)
	DECLARE @ERRORLOG_size INT
	DECLARE @ERRORLOG_age INT
	DECLARE @ERRORLOG_date DATETIME
	DECLARE @cycle_indicator BIT = 0
	DECLARE @ERRORLOG_count SMALLINT = 0


	INSERT into @table_error_logs (log_number, log_date, log_bytes)
	EXEC master.dbo.sp_enumerrorlogs

	SELECT 
		 @ERRORLOG_size = log_bytes
		,@ERRORLOG_date = log_date
		,@ERRORLOG_age  = DATEDIFF(DAY,log_date,GETDATE())
		,@ERRORLOG_size_formatted = CASE 
				WHEN log_bytes BETWEEN 1024 AND 1048575 THEN CONVERT(VARCHAR(10),log_bytes/1024) + 'KB'
				WHEN log_bytes > 1048575 THEN CONVERT(VARCHAR(10),log_bytes/1024/1024) + 'MB'
				ELSE CONVERT(VARCHAR(10),log_bytes) + 'B'
			END
		,@ERRORLOG_max_size_formatted = CASE 
				WHEN @ERRORLOG_max_size_bytes BETWEEN 1024 AND 1048575 THEN CONVERT(VARCHAR(10),@ERRORLOG_max_size_bytes/1024) + 'KB'
				WHEN @ERRORLOG_max_size_bytes > 1048575 THEN CONVERT(VARCHAR(10),@ERRORLOG_max_size_bytes/1024/1024) + 'MB'
				ELSE CONVERT(VARCHAR(10),@ERRORLOG_max_size_bytes) + 'B'
			END
		,@cycle_indicator = CASE 
				WHEN log_bytes > @ERRORLOG_max_size_bytes THEN 1
				WHEN DATEDIFF(DAY,log_date,GETDATE()) > @ERRORLOG_max_age_days THEN 1
				ELSE 0
			END
	FROM @table_error_logs
	WHERE log_number = 0

	SELECT @ERRORLOG_count = COUNT(*) FROM @table_error_logs

	SELECT @message = 'Current ERRORLOG size is ' +  @ERRORLOG_size_formatted + ' and is ' + CONVERT(VARCHAR(10),@ERRORLOG_age) + ' days old. It was created on ' +  CONVERT(VARCHAR(23),@ERRORLOG_date,121) + '.
The current recycle threshold is: ' + @ERRORLOG_max_size_formatted + ' in size OR ' + CONVERT(VARCHAR(10),@ERRORLOG_max_age_days) + ' days in age.
There are ' + CONVERT(VARCHAR(10),@ERRORLOG_count) + ' ERRORLOGS in total.
The ERRORLOG ' + CASE WHEN @cycle_indicator = 1 THEN 'WILL BE' ELSE 'will NOT be' END + ' recycled!'
	PRINT @message

	IF @cycle_indicator = 1
		BEGIN
			EXEC @return_value = sp_cycle_errorlog
			SELECT @output = CASE WHEN @return_value = 0 THEN 'ERRORLOG was sucessfully cycled.' ELSE 'An error occured whilest cycling ERRORLOG.' END
			PRINT @output			
		END
