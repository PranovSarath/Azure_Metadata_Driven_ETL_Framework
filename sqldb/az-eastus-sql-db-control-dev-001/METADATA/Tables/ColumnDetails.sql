CREATE TABLE [METADATA].[ColumnDetails] (
    [ColumnDetailsID] INT            IDENTITY (1, 1) NOT NULL,
    [EntityID]        INT            NOT NULL,
    [ColumnName]      NVARCHAR (100) NOT NULL,
    [DataType]        NVARCHAR (50)  NOT NULL,
    [MaxLength]       INT            NULL,
    [IsNullable]      BIT            DEFAULT ((1)) NOT NULL,
    [Description]     NVARCHAR (255) NULL,
    [LastUpdatedTime] DATETIME       NULL,
    [IsActiveFlag]    BIT            DEFAULT ((1)) NOT NULL,
    [CreatedTime]     DATETIME       DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([ColumnDetailsID] ASC),
    FOREIGN KEY ([EntityID]) REFERENCES [METADATA].[Entity] ([EntityID])
);

