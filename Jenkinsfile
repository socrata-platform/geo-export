@Library('socrata-pipeline-library@generalize-sbt-to-work-for-multiple-projects') _

commonPipeline(
    defaultBuildWorker: 'build-worker',
    jobName: 'geo-export',
    language: 'scala',
    languageOptions: [
	    crossCompile: true,
        isMultiProjectRepository: false,
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
