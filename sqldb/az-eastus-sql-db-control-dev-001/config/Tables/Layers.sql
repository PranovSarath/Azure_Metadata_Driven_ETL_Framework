CREATE TABLE [config].[Layers] (
    [LayerID]        INT           IDENTITY (1, 1) NOT NULL,
    [LayerName]      NVARCHAR (50) NOT NULL,
    [LastUpdateTime] DATETIME      NULL,
    [CreatedTime]    DATETIME      NOT NULL,
    [IsActiveFlag]   BIT           DEFAULT ((1)) NULL,
    PRIMARY KEY CLUSTERED ([LayerID] ASC)
);

