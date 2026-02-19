@Library('socrata-pipeline-library@9.9.1') _

commonPipeline(
    jobName: 'geo-export',
    language: 'scala',
    languageOptions: [
        crossCompile: true,
    ],
    projects: [
        [
            name: 'geo-export',
            compiled: true,
            deploymentEcosystem: 'ecs',
            paths: [
                dockerBuildContext: 'docker',
            ],
            type: 'service',
        ]
    ],
    teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
