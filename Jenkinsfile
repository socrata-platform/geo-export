@Library('socrata-pipeline-library@testing') _

commonPipeline(
    defaultBuildWorker: 'build-worker',
    jobName: 'geo-export',
    language: 'scala',
    languageOptions: [
	version: '2.11',
	crossCompile: true,
	multiProjectBuild: true,
    ],
    projects: [
        [
            name: 'geo-export',
            deploymentEcosystem: 'marathon-mesos',
            type: 'service',
            compile: true  // Sane default
        ]
    ],
    teamsChannelWebhookId: 'WORKFLOW_IQ',
)
