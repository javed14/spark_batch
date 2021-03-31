pipeline {
    agent any

    stages {
        stage('spark-build') {
            steps {
                script {
                    sh """
                    cd  D:/shared_centos/SparkBatching/SparkBatching/sparkBatching/sparkBatching
                    python3 -m venv tutorial-env
                    tutorial-env/Scripts/activate.bat
                    pip install -r requirements.txt
                    python3 main.py
                    """
                }
            }
        }
    }
}