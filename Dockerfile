FROM public.ecr.aws/lambda/python:3.10 

# Copy your source files
COPY src/* ${LAMBDA_TASK_ROOT}

# Copy your requirements file
COPY requirements.txt  ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

