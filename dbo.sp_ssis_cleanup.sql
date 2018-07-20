CREATE PROCEDURE [dbo].[sp_ssis_cleanup] 
(
	@retention_window_length INT = NULL,
	@delete_batch_size INT = 50000
	)
--WITH EXECUTE AS 'AllSchemaOwner'
 WITH ENCRYPTION
AS
EXECUTE AS CALLER;
set nocount on;

DECLARE @sql NVARCHAR(MAX)

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
	------------------------------------------------------------------
	-- This is based on the original Microsoft SSISDB procedure:
	-- [internal].[cleanup_server_retention_window]
	-- However, the original was relying on cascade delete from top
	-- to bottom and for very large databases ususally ended up with
	-- blown transaction log. The top to bottom ratio can be 1:millions
	--
	-- This amended procedure will delete anything less than the desired KEY.
	-- this has been tested on >300GB SSIDB with growth rate of 100GB/month
	-- and was proven much faster than JOIN on key candidates

	-- SSIDB affected by KB 2829948 (http://support.microsoft.com/kb/2829948)
	-- will require missing indexes in addition to:

	 --CREATE NONCLUSTERED INDEX [IX_EventMessageContext_event_message_id]
	 --ON [internal].[event_message_context] ([event_message_id])
	 --INCLUDE ([context_id])

	-- CHANGE LOG:
	--		15/08/2016	Marcin Gminski: 	initial version
	--
	------------------------------------------------------------------
		DECLARE @enable_clean_operation bit

		DECLARE @caller_name nvarchar(256)
		DECLARE @caller_sid  varbinary(85)
		DECLARE @operation_id bigint

		EXECUTE AS CALLER
			SET @caller_name =  SUSER_NAME()
			SET @caller_sid =   SUSER_SID()
		REVERT

		IF OBJECT_ID('tempdb..#DELETE_CANDIDATES') IS NOT NULL
			BEGIN
				DROP TABLE #DELETE_CANDIDATES;
			END;

		CREATE TABLE #DELETE_CANDIDATES (
			operation_id bigint NOT NULL PRIMARY KEY
		);

		BEGIN TRY
			SELECT @enable_clean_operation = CONVERT(bit, property_value) 
				FROM [SSISDB].[catalog].[catalog_properties]
				WHERE property_name = 'OPERATION_CLEANUP_ENABLED'

				IF @enable_clean_operation = 1
					BEGIN
						IF @retention_window_length IS NULL
							BEGIN
								SELECT @retention_window_length = CONVERT(int,property_value)  
									FROM [SSISDB].[catalog].[catalog_properties]
									WHERE property_name = 'RETENTION_WINDOW'
							END

						IF @retention_window_length <= 0 
						BEGIN
							RAISERROR(27163    ,16,1,'RETENTION_WINDOW')
						END

						DECLARE @temp_date datetime
						DECLARE @rows_affected bigint
						DECLARE @deleted_ops TABLE(operation_id bigint, operation_type smallint)
						DECLARE @deleted_size BIGINT
						DECLARE @batchstart DATETIME

						DECLARE @max_operation_id INT
            
						SET @temp_date = GETDATE() - @retention_window_length

						------------------------------------------------------------------
						-- TWEAKED PART:
						------------------------------------------------------------------
						-- get first ID we want to keep, anything below that ID will be
						-- deleted
						------------------------------------------------------------------
						SELECT @max_operation_id = MAX(operation_id) 
						FROM [SSISDB].internal.operations
						WHERE ( [end_time] <= @temp_date
						OR ([end_time] IS NULL AND [status] = 1 AND [created_time] <= @temp_date ))
						PRINT '@max_operation_id: ' + CONVERT(VARCHAR(10),@max_operation_id);
						------------------------------------------------------------------
						-- delete event_message_context
						------------------------------------------------------------------
						SET @deleted_size = 0
						SET @batchstart = GETDATE()
						SET @rows_affected = @delete_batch_size
						WHILE (@rows_affected = @delete_batch_size)
						BEGIN
							DELETE TOP (@delete_batch_size) T 
							FROM [SSISDB].internal.event_message_context AS T
							WHERE T.operation_id < @max_operation_id;
					
							SET @rows_affected = @@ROWCOUNT
							SET @deleted_size = @deleted_size + @rows_affected
						END
						PRINT 'internal.event_message_context: deleted ' + CONVERT(VARCHAR(10),@deleted_size) + ' rows in ' + CONVERT(VARCHAR(50),DATEDIFF(millisecond,@batchstart,GETDATE())) + 'ms' + CHAR(13);
						------------------------------------------------------------------
						-- delete event_messages
						------------------------------------------------------------------
						SET @deleted_size = 0
						SET @rows_affected = @delete_batch_size				
						WHILE (@rows_affected = @delete_batch_size)
						BEGIN
							DELETE TOP (@delete_batch_size) T 
							FROM [SSISDB].internal.event_messages AS T
							WHERE T.operation_id < @max_operation_id;

							SET @rows_affected = @@ROWCOUNT
							SET @deleted_size = @deleted_size + @rows_affected
						END
						PRINT 'internal.event_messages: deleted ' + CONVERT(VARCHAR(10),@deleted_size) + ' rows in ' + CONVERT(VARCHAR(50),DATEDIFF(millisecond,@batchstart,GETDATE())) + 'ms' + CHAR(13);
						------------------------------------------------------------------
						-- delete operation_messages
						------------------------------------------------------------------
						SET @deleted_size = 0
						SET @rows_affected = @delete_batch_size
						WHILE (@rows_affected = @delete_batch_size)
						BEGIN
							DELETE TOP (@delete_batch_size) T 
							FROM [SSISDB].internal.operation_messages AS T
							WHERE T.operation_id < @max_operation_id;

							SET @rows_affected = @@ROWCOUNT
							SET @deleted_size = @deleted_size + @rows_affected
						END
						PRINT 'internal.operation_messages: deleted ' + CONVERT(VARCHAR(10),@deleted_size) + ' rows in ' + CONVERT(VARCHAR(50),DATEDIFF(millisecond,@batchstart,GETDATE())) + 'ms' + CHAR(13);
						------------------------------------------------------------------
						-- delete operations
						------------------------------------------------------------------
						SET @deleted_size = 0
						SET @rows_affected = @delete_batch_size
						WHILE (@rows_affected = @delete_batch_size)
						BEGIN
							DELETE TOP (@delete_batch_size) T 
							OUTPUT DELETED.operation_id, DELETED.operation_type INTO @deleted_ops
							FROM [SSISDB].internal.operations AS T
							WHERE T.operation_id < @max_operation_id;
                    
							SET @rows_affected = @@ROWCOUNT
							SET @deleted_size = @deleted_size + @rows_affected
						END
						PRINT 'internal.operations: deleted ' + CONVERT(VARCHAR(10),@deleted_size) + ' rows in ' + CONVERT(VARCHAR(50),DATEDIFF(millisecond,@batchstart,GETDATE())) + 'ms' + CHAR(13);
						------------------------------------------------------------------
						-- END OF TWEAKED PART
						-- BELOW STANDARD MS PROC
						------------------------------------------------------------------
						DECLARE @execution_id bigint
						DECLARE @sqlString              nvarchar(1024)
						DECLARE @key_name               nvarchar(1024)
						DECLARE @certificate_name       nvarchar(1024)
            
            
						DECLARE execution_cursor CURSOR LOCAL FOR 
							SELECT operation_id FROM @deleted_ops 
							WHERE operation_type = 200
            
						OPEN execution_cursor
						FETCH NEXT FROM execution_cursor INTO @execution_id
            
						WHILE @@FETCH_STATUS = 0
						BEGIN
							SET @key_name = 'MS_Enckey_Exec_'+CONVERT(varchar,@execution_id)
							SET @certificate_name = 'MS_Cert_Exec_'+CONVERT(varchar,@execution_id)
							SET @sqlString = 'USE [SSISDB]; IF EXISTS (SELECT name FROM sys.symmetric_keys WHERE name = ''' + @key_name +''') '
								+'DROP SYMMETRIC KEY '+ @key_name
								EXECUTE sp_executesql @sqlString
							SET @sqlString = 'USE [SSISDB]; IF EXISTS (select name from sys.certificates WHERE name = ''' + @certificate_name +''') '
								+'DROP CERTIFICATE '+ @certificate_name
								EXECUTE sp_executesql @sqlString
							FETCH NEXT FROM execution_cursor INTO @execution_id
						END
						CLOSE execution_cursor
						DEALLOCATE execution_cursor
            
						UPDATE [SSISDB].[internal].[operations]
							SET [status] = 7,
							[end_time] = SYSDATETIMEOFFSET()
							WHERE [operation_id] = @operation_id                              

					END
		END TRY
			BEGIN CATCH
        
        
				IF (CURSOR_STATUS('local', 'execution_cursor') = 1 
					OR CURSOR_STATUS('local', 'execution_cursor') = 0)
				BEGIN
					CLOSE execution_cursor
					DEALLOCATE execution_cursor            
				END
        
				UPDATE [SSISDB].[internal].[operations]
					SET [status] = 4,
					[end_time] = SYSDATETIMEOFFSET()
					WHERE [operation_id] = @operation_id;       
				THROW
			END CATCH
