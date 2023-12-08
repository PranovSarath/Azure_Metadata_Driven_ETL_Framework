CREATE TABLE [METADATA].[Test] (
    [TestID]      INT           IDENTITY (1, 1) NOT NULL,
    [TestName]    NVARCHAR (50) NOT NULL,
    [CreatedTime] DATETIME      DEFAULT (getdate()) NULL,
    PRIMARY KEY CLUSTERED ([TestID] ASC)
);

