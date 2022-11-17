FROM 715267777840.dkr.ecr.ap-south-1.amazonaws.com/dbt-glue:v1
USER dbt
COPY --chown=dbt:dbt . /dbt/
RUN rm -rf /dbt/logs /dbt/target
WORKDIR /dbt
CMD [ "run", "--profiles-dir", "/dbt" ]
ENTRYPOINT [ "dbt"]