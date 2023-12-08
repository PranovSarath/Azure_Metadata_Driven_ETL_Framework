CREATE TABLE [ETL].[PipelineRuns] (
    [PipelineRunsID]     INT            IDENTITY (1, 1) NOT NULL,
    [PipelineName]       NVARCHAR (140) NOT NULL,
    [RunID]              NVARCHAR (255) NOT NULL,
    [ExecutionStartTime] DATETIME       NOT NULL,
    [ExecutionEndTime]   DATETIME       NULL,
    [RunStatusID]        INT            NOT NULL,
    [LastUpdateTime]     DATETIME       NULL,
    [ParametersList]     NVARCHAR (MAX) NULL,
    [CreatedTime]        DATETIME       DEFAULT (getdate()) NOT NULL,
    PRIMARY KEY CLUSTERED ([PipelineRunsID] ASC),
    FOREIGN KEY ([RunStatusID]) REFERENCES [config].[Status] ([StatusID])
);

