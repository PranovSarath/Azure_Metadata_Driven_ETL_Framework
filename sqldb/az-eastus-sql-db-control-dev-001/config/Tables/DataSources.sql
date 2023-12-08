CREATE TABLE [config].[DataSources] (
    [DataSourceID]   INT           IDENTITY (1, 1) NOT NULL,
    [DataSourceName] NVARCHAR (50) NOT NULL,
    [LastUpdateTime] DATETIME      NULL,
    [CreatedTime]    DATETIME      NOT NULL,
    [IsActiveFlag]   BIT           DEFAULT ((1)) NULL,
    PRIMARY KEY CLUSTERED ([DataSourceID] ASC)
);

