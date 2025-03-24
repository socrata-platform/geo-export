@Library('socrata-pipeline-library@3.0.0') _

commonPipeline(
    defaultBuildWorker: 'build-worker',
    jobName: 'geo-export',
    language: 'scala',
    languageVersion: '2.12',
    projects: [
        [
            name: 'geo-export',
            deploymentEcosystem: 'marathon-mesos',
            type: 'service',
        ]
    ],
    teamsChannelWebhookId: 'WORKFLOW_IQ',
)
