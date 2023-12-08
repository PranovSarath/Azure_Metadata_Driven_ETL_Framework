CREATE TABLE [config].[LoadType] (
    [LoadTypeID]     INT           IDENTITY (1, 1) NOT NULL,
    [LoadType]       NVARCHAR (50) NOT NULL,
    [LastUpdatetime] DATETIME      NULL,
    [CreatedTime]    DATETIME      DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([LoadTypeID] ASC)
);

