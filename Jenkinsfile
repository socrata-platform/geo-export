@Library('socrata-pipeline-library@9.7.0') _

commonPipeline(
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
    teamsChannelWebhookId: 'WORKFLOW_EGRESS_AUTOMATION',
)
