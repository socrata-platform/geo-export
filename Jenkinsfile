@Library('socrata-pipeline-library@7.0.0') _

commonPipeline(
    defaultBuildWorker: 'build-worker',
    jobName: 'geo-export',
    language: 'scala',
    languageOptions: [
        crossCompile: true,
    ],
    projects: [
        [
            name: 'geo-export',
            deploymentEcosystem: 'marathon-mesos',
            type: 'service',
            compiled: true,
            paths: [
                dockerBuildContext: 'docker'
            ]
        ]
    ],
    teamsChannelWebhookId: 'WORKFLOW_IQ',
)
