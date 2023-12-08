CREATE TABLE [config].[Status] (
    [StatusID]       INT           IDENTITY (1, 1) NOT NULL,
    [Status]         NVARCHAR (50) NOT NULL,
    [IsActiveFlag]   BIT           DEFAULT ((1)) NOT NULL,
    [LastUpdateTime] DATETIME      NULL,
    [CreatedTime]    DATETIME      DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([StatusID] ASC)
);

