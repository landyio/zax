package io.landy.app.util

import java.io.ByteArrayInputStream

import com.amazonaws.auth.PropertiesFileCredentialsProvider
import com.amazonaws.regions.{Regions, Region}
import com.amazonaws.services.s3.model.{CannedAccessControlList, PutObjectRequest, ObjectMetadata}
import com.amazonaws.services.s3.{S3ClientOptions, AmazonS3Client}
import com.typesafe.config.ConfigFactory

object AWS {

  val config = ConfigFactory.load()

  object S3 {

    val bucket = "zax.landy.io"

    /**
      * Uploads given data to the S3
      *
      * @param data data to be uploaded
      * @return     url of the data uploaded
      */
    def uploadTo(data: Array[Byte], path: String): String = {
      val s3 = new AmazonS3Client(
        new PropertiesFileCredentialsProvider(config.getString("aws.credentials.path")).getCredentials
      )

      val key = s"samples/$path"

      val opts = new S3ClientOptions()

      opts.setPathStyleAccess(true)

      s3.setRegion(Region.getRegion(Regions.US_WEST_2))
      s3.setS3ClientOptions(opts)
      s3.putObject(
        new PutObjectRequest(bucket, key, new ByteArrayInputStream(data), new ObjectMetadata())
              .withCannedAcl(CannedAccessControlList.PublicRead)
      )
      s3.getResourceUrl(bucket, key)
    }

  }
}
