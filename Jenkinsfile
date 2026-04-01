@Library('socrata-pipeline-library@10.2.1') _

commonPipeline(
    jobName: 'geo-export',
    languageOptions: [
        scala: [
            crossCompile: true,
        ],
    ],
    projects: [
        [
            name: 'geo-export',
            compiled: true,
            deploymentEcosystem: 'ecs',
            docker: [
                buildContext: 'docker',
            ],
            language: 'scala',
            type: 'service',
        ]
    ],
    teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
