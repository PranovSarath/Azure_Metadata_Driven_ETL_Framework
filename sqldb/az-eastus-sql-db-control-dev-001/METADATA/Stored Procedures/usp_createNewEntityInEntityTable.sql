CREATE PROCEDURE [METADATA].[usp_createNewEntityInEntityTable] 
@EntityName NVARCHAR(100),
@SubjectName NVARCHAR(50),
@Layer NVARCHAR(50),
@DataSourceName NVARCHAR(50),
@LoadType NVARCHAR(50) = 'FullLoad',
@EntityType NVARCHAR(50) = 'Transaction',
@EntityDescription NVARCHAR(max) = '',
@SubjectDescription NVARCHAR(max) = '',
@SourcePath NVARCHAR(255) = '',
@DestinationPath NVARCHAR(255) = '',
@BusinessArea NVARCHAR(80) = ''
AS
BEGIN
	--Declare Variables
	DECLARE @DataSourceID INT;
	DECLARE @LayerID INT;
	DECLARE @SubjectID INT;
	DECLARE @LoadTypeID INT;
	DECLARE @EntityTypeID INT;


	--Retrieve the DataSourceID value
	SET @DataSourceID = (SELECT DataSourceID FROM [config].[DataSources] where DataSourceName = @DataSourceName);
	--Retrieve the LayerID value
	SET @LayerID = (SELECT LayerID from [config].[Layers] where LayerName = @Layer);
	--Retrieve LoadTypeID value
	SET @LoadTypeID = (SELECT LoadTypeID from [config].[LoadType] where LoadType = @LoadType);
	--Retrieve EntityTypeID value
	SET @EntityTypeID = (SELECT EntityTypeID from [config].[EntityType] where EntityType = @EntityType);


	--Check if subject is already existing in the table
	IF EXISTS (SELECT 1 from [METADATA].[Subject] where SubjectName = @SubjectName and LayerID = @LayerID)
	BEGIN
		SELECT @SubjectID = SubjectID from [METADATA].[Subject] where SubjectName = @SubjectName and LayerID = @LayerID;
	END
	ELSE
	BEGIN
		INSERT INTO [METADATA].[Subject] (SubjectName, DataSourceID, LayerID, Description, BusinessArea, CreatedTime)
		VALUES (@SubjectName, @DataSourceID, @LayerID, @SubjectDescription, @BusinessArea, GETDATE());

		SELECT @SubjectID = SubjectID from [METADATA].[Subject] where SubjectName = @SubjectName and LayerID = @LayerID;
	END


	--Insert New Entity to the Entity Table
	IF EXISTS (SELECT 1 from [METADATA].[Entity] where EntityName = @EntityName and SubjectID = @SubjectID)
	BEGIN
		PRINT ('An entity record of the same name and belonging to the same subject already exists');
	END
	ELSE
	BEGIN
		INSERT INTO [METADATA].[Entity] (EntityName, SubjectID, Description, CreatedTime, SourcePath, DestinationPath, LoadTypeID, LoadType, EntityTypeID, EntityType)
		VALUES (@EntityName, @SubjectID, @EntityDescription, GETDATE(), @SourcePath, @DestinationPath, @LoadTypeID, @LoadType, @EntityTypeID, @EntityType);
	END
END
