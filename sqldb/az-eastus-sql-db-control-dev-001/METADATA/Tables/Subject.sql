CREATE TABLE [METADATA].[Subject] (
    [SubjectID]      INT            IDENTITY (1, 1) NOT NULL,
    [SubjectName]    NVARCHAR (50)  NOT NULL,
    [DataSourceID]   INT            NULL,
    [LayerID]        INT            NULL,
    [Description]    NVARCHAR (MAX) NULL,
    [BusinessArea]   NVARCHAR (80)  NULL,
    [IsActive]       BIT            DEFAULT ((1)) NOT NULL,
    [LastUpdateTime] DATETIME       NULL,
    [CreatedTime]    DATETIME       NOT NULL,
    PRIMARY KEY CLUSTERED ([SubjectID] ASC),
    FOREIGN KEY ([DataSourceID]) REFERENCES [config].[DataSources] ([DataSourceID]),
    FOREIGN KEY ([LayerID]) REFERENCES [config].[Layers] ([LayerID])
);

