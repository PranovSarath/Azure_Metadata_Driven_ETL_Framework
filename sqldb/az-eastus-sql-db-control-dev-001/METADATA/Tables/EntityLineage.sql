CREATE TABLE [METADATA].[EntityLineage] (
    [EntityLineageID]   INT      IDENTITY (1, 1) NOT NULL,
    [EntityID]          INT      NOT NULL,
    [DependsOnEntityID] INT      NULL,
    [IsActiveFlag]      BIT      DEFAULT ((1)) NOT NULL,
    [LastUpdateTime]    DATETIME NULL,
    [CreatedTime]       DATETIME NULL,
    PRIMARY KEY CLUSTERED ([EntityLineageID] ASC),
    FOREIGN KEY ([DependsOnEntityID]) REFERENCES [METADATA].[Entity] ([EntityID]),
    FOREIGN KEY ([EntityID]) REFERENCES [METADATA].[Entity] ([EntityID])
);

