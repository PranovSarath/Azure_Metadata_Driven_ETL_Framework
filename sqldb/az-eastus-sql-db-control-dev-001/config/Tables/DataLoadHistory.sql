CREATE TABLE [config].[DataLoadHistory] (
    [DataLoadHistoryID]   INT            IDENTITY (1, 1) NOT NULL,
    [DataLoadName]        NVARCHAR (50)  NOT NULL,
    [DataLoadDescription] NVARCHAR (MAX) NULL,
    [SubjectID]           INT            NOT NULL,
    [Subject]             NVARCHAR (50)  NOT NULL,
    [LastUpdateTime]      DATETIME       NULL,
    [CreatedTime]         DATETIME       DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([DataLoadHistoryID] ASC),
    FOREIGN KEY ([SubjectID]) REFERENCES [METADATA].[Subject] ([SubjectID]),
    UNIQUE NONCLUSTERED ([DataLoadName] ASC)
);

