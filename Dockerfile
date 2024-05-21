FROM public.ecr.aws/lambda/python:3.12

# Copy your requirements file
COPY requirements.txt  ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Copy your source files
COPY . ${LAMBDA_TASK_ROOT}

# CMD src/main_prefect.lambda_handler 

# CMD ["prefect", "server", "start"]

# # Run lambda_handler when the container starts
CMD ["src/main_prefect.lambda_handler"]
