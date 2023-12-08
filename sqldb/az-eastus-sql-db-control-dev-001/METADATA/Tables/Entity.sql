CREATE TABLE [METADATA].[Entity] (
    [EntityID]         INT            IDENTITY (1, 1) NOT NULL,
    [EntityName]       NVARCHAR (100) NOT NULL,
    [SubjectID]        INT            NULL,
    [Description]      NVARCHAR (MAX) NULL,
    [IsActive]         BIT            DEFAULT ((1)) NOT NULL,
    [LastUpdateTime]   DATETIME       NULL,
    [CreatedTime]      DATETIME       NOT NULL,
    [SourcePath]       NVARCHAR (255) NULL,
    [DestinationPath]  NVARCHAR (255) NULL,
    [LoadTypeID]       INT            NULL,
    [LoadType]         NVARCHAR (50)  NULL,
    [EntityTypeID]     INT            NULL,
    [EntityType]       NVARCHAR (50)  NULL,
    [NotebookPath]     NVARCHAR (255) NULL,
    [BusinessKey]      NVARCHAR (255) NULL,
    [IdColumn]         NVARCHAR (50)  NULL,
    [PartitionColumns] NVARCHAR (100) NULL,
    PRIMARY KEY CLUSTERED ([EntityID] ASC),
    FOREIGN KEY ([EntityTypeID]) REFERENCES [config].[EntityType] ([EntityTypeID]),
    FOREIGN KEY ([LoadTypeID]) REFERENCES [config].[LoadType] ([LoadTypeID]),
    FOREIGN KEY ([SubjectID]) REFERENCES [METADATA].[Subject] ([SubjectID])
);

