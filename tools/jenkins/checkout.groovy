private def gitCheckout(String repoUrl, String credential, String refname) {
    checkout([
        $class: 'GitSCM',
        branches: [[name: "$refname"]],
        userRemoteConfigs: [[
            url: repoUrl,
            credentialsId: credential,
        ]]
    ])
}

def checkout_dq_measures_python(String refname) {
    // https://jenkins.macbisdw.cmscloud.local/credentials/store/system/domain/_/credential/9ce68047-34f2-4331-98ea-5f52349d2049/
    credentialsId = '9ce68047-34f2-4331-98ea-5f52349d2049'
    gitCheckout('git@github.com:tmsis/T-MSIS-Analytic-File-Generation-Python.git', credentialsId, refname)
}

return this