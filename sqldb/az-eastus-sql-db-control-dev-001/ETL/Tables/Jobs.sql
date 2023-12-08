CREATE TABLE [ETL].[Jobs] (
    [JobID]                INT           IDENTITY (1, 1) NOT NULL,
    [DataLoadHistoryID]    INT           NOT NULL,
    [PipelineRunsID]       INT           NOT NULL,
    [SourceSubjectID]      INT           NOT NULL,
    [SourceSubject]        NVARCHAR (50) NOT NULL,
    [DestinationSubjectID] INT           NOT NULL,
    [DestinationSubject]   NVARCHAR (50) NOT NULL,
    [StatusID]             INT           NOT NULL,
    [LastUpdateTime]       DATETIME      NULL,
    [CreatedTime]          DATETIME      DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([JobID] ASC),
    FOREIGN KEY ([DataLoadHistoryID]) REFERENCES [config].[DataLoadHistory] ([DataLoadHistoryID]),
    FOREIGN KEY ([DestinationSubjectID]) REFERENCES [METADATA].[Subject] ([SubjectID]),
    FOREIGN KEY ([PipelineRunsID]) REFERENCES [ETL].[PipelineRuns] ([PipelineRunsID]),
    FOREIGN KEY ([SourceSubjectID]) REFERENCES [METADATA].[Subject] ([SubjectID]),
    FOREIGN KEY ([StatusID]) REFERENCES [config].[Status] ([StatusID])
);

