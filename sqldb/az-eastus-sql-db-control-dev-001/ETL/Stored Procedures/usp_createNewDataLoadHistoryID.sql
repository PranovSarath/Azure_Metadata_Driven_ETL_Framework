CREATE PROCEDURE [ETL].[usp_createNewDataLoadHistoryID] 
@DataHistoryDescription NVARCHAR(max),
@PipelineName NVARCHAR(140),
@SourceSubjectName NVARCHAR(50),
@DestinationSubjectName NVARCHAR(100),
@Status NVARCHAR(50),
@RunID NVARCHAR(255),
@ParametersList NVARCHAR(max),
@ExecutionStartTime datetime = NULL
--@DataHistoryID INT OUTPUT
AS
BEGIN
	--Declare the required variables
	DECLARE @DataHistoryID INT;
	DECLARE @DataLoadName NVARCHAR(50);
	DECLARE @DataLoadVersion VARCHAR(4);
	DECLARE @SubjectID INT;
	DECLARE @RunStatusID INT;
	DECLARE @PipelineRunID INT;
	DECLARE @DestSubjectID INT;


	--Retrieve RunStatusID 
	SET @RunStatusID = (SELECT StatusID from [config].[Status] where Status = 'In Progress');

	--Set value of ExecutionStartTime if value is NULL
	IF @ExecutionStartTime IS NULL
		SET @ExecutionStartTime = GETDATE();

	--Retrieve the SubjectID from the [METADATA].[Subject] table
	SET @SubjectID = (SELECT TOP 1 SubjectID from [METADATA].[Subject] where [SubjectName] = @SourceSubjectName and LayerID = 1);
	--Retrieve the SubjectID from the [METADATA].[Subject] table
	SET @DestSubjectID = (SELECT TOP 1 SubjectID from [METADATA].[Subject] where [SubjectName] = @DestinationSubjectName and LayerID = 2);

	--Retrieve DataLoadVersion
	SET @DataLoadVersion = (SELECT count(*) AS count from [config].[DataLoadHistory] WHERE CAST(CreatedTime AS DATE) = CAST(GETDATE() AS DATE) AND Subject like CONCAT('%', @SourceSubjectName, '%'));
	SET @DataLoadVersion = @DataLoadVersion + 1;
	SET @DataLoadName = @SourceSubjectName + '_' + CONVERT(VARCHAR(8), GETDATE(), 112) + '_' + RIGHT('00' + CAST(@DataLoadVersion AS VARCHAR(2)), 2); 
	
	PRINT (@DataLoadName)
	--Create a new entry in the DataHistoryTable
	INSERT INTO [config].[DataLoadHistory] ([DataLoadName], [DataLoadDescription], [SubjectID], [Subject])
	VALUES (@DataLoadName, @DataHistoryDescription, @SubjectID, @SourceSubjectName);
	PRINT ('New record created in the DataLoadHistoryTable')

	--Lookup the latest DataHistory value
	SET @DataHistoryID = (SELECT DataLoadHistoryID from [config].[DataLoadHistory] where DataLoadName = @DataLoadName);
	PRINT(@DataHistoryID)

	--Create new record for the pipeline run in the PipelineRuns table
	INSERT INTO [ETL].[PipelineRuns] (PipelineName, RunID, ExecutionStartTime, RunStatusID, LastUpdateTime, ParametersList)
	VALUES (@PipelineName, @RunID, @ExecutionStartTime, @RunStatusID, GETDATE(), @ParametersList);
	PRINT ('New record created in the PipelineRuns table')

	--Retrieve the latest PipelineRunID from the table [ETL].[PipelineRuns]
	SET @PipelineRunID = (SELECT TOP 1 PipelineRunsID from [ETL].[PipelineRuns] where RunID = @RunID);

	--Create a new record for the job in the [ETL].[Jobs] table
	INSERT INTO [ETL].[Jobs] ([DataLoadHistoryID], [PipelineRunsID], [SourceSubjectID], [SourceSubject], [DestinationSubjectID], [DestinationSubject], [StatusID])
	VALUES (@DataHistoryID, @PipelineRunID, @SubjectID, @SourceSubjectName, @DestSubjectID, @DestinationSubjectName, @RunStatusID);
	PRINT ('New record created in the Jobs table')

	SELECT * FROM [ETL].[Jobs] where [DataLoadHistoryID] = @DataHistoryID; 



END
