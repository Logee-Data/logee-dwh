cataloger_docker_image ?= sagaekakristi/gcp-data-cataloger
cataloger_docker_tag ?= 1.0.1
env ?= logee-data-dev
dryrun ?= true
linux_user_name = $(shell whoami)
linux_user_uid = $(shell id -u $(linux_user_name))
linux_user_gid = $(shell id -g $(linux_user_name))

# test
cataloger_test: cataloger_test_pyflakes

cataloger_test_pyflakes: cataloger_test_pyflakes_src cataloger_test_pyflakes_tests

cataloger_test_pyflakes_src:
	docker run -it --rm \
	-v $(shell pwd):/workspace \
	--entrypoint pyflakes \
	eeacms/pyflakes:py3 \
	/workspace/scripts/data_cataloger/data_cataloger/

cataloger_test_pyflakes_tests:
	docker run -it --rm \
	-v $(shell pwd):/workspace \
	--entrypoint pyflakes \
	eeacms/pyflakes:py3 \
	/workspace/scripts/data_cataloger/tests/

cataloger_test_unit:
	docker run -it --rm \
	-v $(shell pwd):/workspace \
	-e PYTHONPATH=/workspace \
	--entrypoint pytest \
	$(cataloger_docker_image):$(cataloger_docker_tag) \
	--cov=data_cataloger \
	--cov-config=/workspace/.coveragerc \
	--cov-report term-missing \
	/workspace/scripts/data_cataloger/tests/unit

# docker
cataloger_docker_build: cataloger_docker_general_build

cataloger_cataloger_docker_general: cataloger_docker_general_build cataloger_docker_general_push

cataloger_docker_general_build:
	docker build \
	-f scripts/data_cataloger/docker/general/Dockerfile \
	-t $(cataloger_docker_image):$(cataloger_docker_tag) \
	.

cataloger_docker_general_push:
	docker push $(cataloger_docker_image):$(cataloger_docker_tag)

# run
cataloger_run_local:
	docker run -it --rm \
	--network host \
	--user $(linux_user_uid):$(linux_user_gid) \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/scripts/data_cataloger/credentials/service_account.$(env).json \
	-v /tmp:/tmp \
	-v $(shell pwd):/workspace:ro \
	--entrypoint /usr/local/bin/python \
	$(cataloger_docker_image):$(cataloger_docker_tag) \
	/workspace/scripts/data_cataloger/data_cataloger/cataloger.py \
	sync-data-catalog \
	--parameters_path=/workspace/scripts/data_cataloger/parameters/parameters.$(env).yaml \
	--service_account_path=/workspace/scripts/data_cataloger/credentials/service_account.$(env).json \
	--dryrun=$(dryrun)

# for debugging, trial & error
cataloger_shell:
	docker run -it --rm \
	--network host \
	--user $(linux_user_uid):$(linux_user_gid) \
	-e GOOGLE_APPLICATION_CREDENTIALS=/workspace/scripts/data_cataloger/credentials/service_account.$(env).json \
	-v /tmp:/tmp \
	-v $(shell pwd):/workspace:ro \
	--entrypoint bash \
	$(cataloger_docker_image):$(cataloger_docker_tag)
