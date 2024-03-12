import boto3
import requests
from io import BytesIO


class AmazonS3:
    VALID_ACL = [
        "private",
        "public-read",
        "public-read-write",
        "authenticated-read",
        "aws-exec-read",
        "bucket-owner-read",
        "bucket-owner-full-control",
        "log-delivery-write",
    ]

    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, endpoint_url=None
    ) -> None:
        """
        @type aws_access_key_id: str
        @param aws_access_key_id: AWS's access key ID.
        @type aws_secret_access_key: str
        @param aws_secret_access_key: AWS's secret access key.
        @rtype: None
        """
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.s3_raw_client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            endpoint_url=endpoint_url,
            aws_secret_access_key=aws_secret_access_key,
        )

    def _check_acl(self, acl: str) -> None:
        """
        Check if the ACL attribute is valid.
        @type acl: str
        @param acl: The ACL attribute, which can be referred from https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl.
        @return: None
        """
        if acl and acl not in self.VALID_ACL:
            raise ValueError(
                f"Invalid ACL: {acl}. Please check the https://docs.aws.amazon.com/AmazonS3/latest/userguide/acl-overview.html#canned-acl for valid ACLs."
            )

    def upload_from_local_file(
        self,
        file_path: str,
        bucket_name: str,
        object_name: str,
        acl: str = "public-read",
        bucket_domain: str = "",
    ) -> str:
        """
        Upload a file from local to S3, and return the public URL of the uploaded file.
        @type file_path: str
        @param file_path: The absolute path of the file to be uploaded.
        @type bucket_name: str
        @param bucket_name: The name of the target Amazon S3 bucket.
        @type object_name: str
        @param object_name: The key/name/path of the object in the S3 bucket. For example, a/b.txt is the b.txt file under the `a` folder.
        @type acl: str
        @param acl: The ACL attribute of the object. Default is public-read.
        @type bucket_domain: str
        @param bucket_domain: The domain of the S3 bucket, which is used to replace the default URL. For example, s3.yourdomain.com.
        @rtype: str
        @return: The public URL of the uploaded file.
        """
        self.s3_raw_client.upload_file(
            file_path, bucket_name, object_name, ExtraArgs={"ACL": acl}
        )

        # Construct the public URL of the image in S3
        public_url = (
            f"https://{bucket_domain}/{object_name}"
            if bucket_domain
            else f"https://{bucket_name}.s3.amazonaws.com/{object_name}"
        )
        return public_url

    def upload_from_url(
        self,
        url: str,
        bucket_name: str,
        object_name: str,
        acl: str = "public-read",
        bucket_domain: str = "",
    ) -> str:
        """
        Download the file from the URL, and then upload to S3. Finally return the public URL of the uploaded file.
        @type url: str
        @param url: The file URL to be downloaded.
        @type bucket_name: str
        @param bucket_name:  The name of the target Amazon S3 bucket.
        @type object_name: str
        @param object_name: The key/name/path of the object in the S3 bucket. For example, a/b.txt is the b.txt file under the `a` folder.
        @type acl: str
        @param acl: The ACL attribute of the object. Default is public-read.
        @type bucket_domain: str
        @param bucket_domain: The domain of the S3 bucket, which is used to replace the default URL. For example, s3.yourdomain.com.
        @rtype: str
        @return: The public URL of the uploaded file.
        """
        response = requests.get(url)
        response.raise_for_status()
        self.s3_raw_client.put_object(
            Bucket=bucket_name, Key=object_name, Body=BytesIO(response.content), ACL=acl
        )

        # Construct the public URL of the image in S3
        public_url = (
            f"https://{bucket_domain}/{object_name}"
            if bucket_domain
            else f"https://{bucket_name}.s3.amazonaws.com/{object_name}"
        )
        return public_url


class CloudflareR2(AmazonS3):
    def __init__(
        self, aws_access_key_id: str, aws_secret_access_key: str, endpoint_url: str
    ) -> None:
        """
        @type aws_access_key_id:
        @param aws_access_key_id
        @type aws_secret_access_key:
        @param aws_secret_access_key:
        @type endpoint_url:str
        @param endpoint_url:check your bucket /settings -> Bucket Details -> S3 API for the endpoint, do not include the stuff after '.com'
        """
        super().__init__(aws_access_key_id, aws_secret_access_key, endpoint_url)
