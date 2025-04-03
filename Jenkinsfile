@Library('socrata-pipeline-library@generalize-sbt-to-work-for-multiple-projects') _

commonPipeline(
    defaultBuildWorker: 'build-worker',
    jobName: 'geo-export',
    language: 'scala',
    languageOptions: [
	    crossCompile: true,
	    multiProjectBuild: false,
    ],
    projects: [
        [
            name: 'geo-export',
            deploymentEcosystem: 'marathon-mesos',
            type: 'service',
            compiled: true
        ]
    ],
    teamsChannelWebhookId: 'WORKFLOW_IQ',
)
