CREATE TABLE [config].[EntityType] (
    [EntityTypeID]   INT           IDENTITY (1, 1) NOT NULL,
    [EntityType]     NVARCHAR (50) NOT NULL,
    [LastUpdatetime] DATETIME      NULL,
    [CreatedTime]    DATETIME      DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([EntityTypeID] ASC)
);

