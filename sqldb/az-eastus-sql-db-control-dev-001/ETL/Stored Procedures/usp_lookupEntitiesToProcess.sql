CREATE PROCEDURE [ETL].[usp_lookupEntitiesToProcess]
@SourceSubject NVARCHAR(50),
@DestSubject NVARCHAR(50),
@SourceLayer NVARCHAR(50),
@DestLayer NVARCHAR(50)
AS
BEGIN
	DECLARE @SourceLayerID INT;
	DECLARE @DestLayerID INT;

	SET @SourceLayerID = (SELECT LayerID from [config].[Layers] where [LayerName] = @SourceLayer);
	SET @DestLayerID = (SELECT LayerID from [config].[Layers] where [LayerName] = @DestLayer);
	PRINT (@SourceLayerID)
	PRINT(@DestLayerID)

	SELECT ms.SubjectName as SourceSubject, ms.SubjectID as SourceSubjectID, ms.BusinessArea, me.EntityName as SourceEntityName,
	me.EntityID as SourceEntityID, me.SourcePath, me.DestinationPath, me.LoadType, me.EntityType,
	ms2.SubjectName as DestinationSubject, ms2.SubjectID as DestinationSubjectID, me2.EntityName as DestinationEntity, me2.BusinessKey, me2.NotebookPath,
	me2.EntityID as DestinationEntityID, @SourceLayer as SourceLayer, @DestLayer as DestinationLayer
	FROM [METADATA].[Subject] ms
	INNER JOIN [METADATA].[Entity] me
	on ms.SubjectID = me.SubjectID
	INNER JOIN [METADATA].[EntityLineage] mel
	on mel.DependsOnEntityID = me.EntityID
	INNER JOIN [METADATA].[Entity] me2
	on me2.EntityID = mel.EntityID
	INNER JOIN [METADATA].[Subject] ms2
	on ms2.SubjectID = me2.SubjectID
	where ms.SubjectName = @SourceSubject and ms.LayerID = @SourceLayerID
	and ms2.SubjectName = @DestSubject and ms2.LayerID = @DestLayerID
	and me.IsActive = 1;
END
