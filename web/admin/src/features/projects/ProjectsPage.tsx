import { useState } from "react";
import { App, Alert, Button, Result, Space, Spin, Typography } from "antd";
import {
  useAssignProjectRuleSetMutation,
  useDeleteProjectRuleSetAssignmentMutation,
  useProjectRuleSetAssignmentsQuery,
  useRuleSetsQuery,
} from "../rules/hooks";
import { ProjectRuleSetsPanel } from "../rules/ProjectRuleSetsPanel";
import {
  getErrorMessage as getRuleErrorMessage,
} from "../rules/utils";
import {
  useCreateProjectMutation,
  useDeleteProjectMutation,
  useProjectsQuery,
  useUpdateProjectMutation,
} from "./hooks";
import { ProjectFormModal } from "./ProjectFormModal";
import { ProjectsTable } from "./ProjectsTable";
import type { Project, ProjectFormValues } from "./types";
import {
  getErrorMessage,
  toCreateProjectPayload,
  toUpdateProjectPayload,
} from "./utils";

export function ProjectsPage() {
  const { message } = App.useApp();
  const projectsQuery = useProjectsQuery();
  const createProjectMutation = useCreateProjectMutation();
  const updateProjectMutation = useUpdateProjectMutation();
  const deleteProjectMutation = useDeleteProjectMutation();
  const ruleSetsQuery = useRuleSetsQuery();
  const projects = projectsQuery.data ?? [];
  const ruleSets = ruleSetsQuery.data ?? [];
  const hasLoadedProjects = projectsQuery.data !== undefined;
  const showInitialError = projectsQuery.isError && !hasLoadedProjects;
  const showRefreshError = projectsQuery.isError && hasLoadedProjects;
  const [modalMode, setModalMode] = useState<"create" | "edit">("create");
  const [editingProject, setEditingProject] = useState<Project | null>(null);
  const [isFormOpen, setIsFormOpen] = useState(false);
  const [deletingAppid, setDeletingAppid] = useState<string | null>(null);
  const [updatingRuleSetId, setUpdatingRuleSetId] = useState<number | null>(null);
  const editingAppid =
    isFormOpen && modalMode === "edit" ? editingProject?.appid ?? null : null;
  const assignmentsQuery = useProjectRuleSetAssignmentsQuery(editingAppid);
  const assignRuleSetMutation = useAssignProjectRuleSetMutation(editingAppid);
  const deleteAssignmentMutation =
    useDeleteProjectRuleSetAssignmentMutation(editingAppid);

  const resetFormMutationState = () => {
    createProjectMutation.reset();
    updateProjectMutation.reset();
  };

  const handleCreateClick = () => {
    if (deletingAppid) {
      return;
    }

    resetFormMutationState();
    setModalMode("create");
    setEditingProject(null);
    setIsFormOpen(true);
  };

  const handleEditClick = (project: Project) => {
    if (deletingAppid) {
      return;
    }

    resetFormMutationState();
    setModalMode("edit");
    setEditingProject(project);
    setIsFormOpen(true);
  };

  const handleCloseModal = () => {
    if (createProjectMutation.isPending || updateProjectMutation.isPending) {
      return;
    }

    resetFormMutationState();
    setIsFormOpen(false);
    setEditingProject(null);
  };

  const handleSubmit = async (values: ProjectFormValues) => {
    try {
      if (modalMode === "create") {
        await createProjectMutation.mutateAsync(toCreateProjectPayload(values));
        message.success(`项目 ${values.appid} 创建成功`);
      } else if (editingProject) {
        await updateProjectMutation.mutateAsync({
          appid: editingProject.appid,
          payload: toUpdateProjectPayload(values),
        });
        message.success(`项目 ${editingProject.appid} 保存成功`);
      }

      setIsFormOpen(false);
      setEditingProject(null);
    } catch (error) {
      message.error(
        getErrorMessage(
          error,
          modalMode === "create"
            ? "创建项目失败，请稍后重试。"
            : "保存项目失败，请稍后重试。",
        ),
      );
      throw error;
    }
  };

  const handleDelete = async (project: Project) => {
    if (deletingAppid) {
      return;
    }

    setDeletingAppid(project.appid);

    try {
      await deleteProjectMutation.mutateAsync(project.appid);
      message.success(`项目 ${project.appid} 删除成功`);
    } catch (error) {
      message.error(
        getErrorMessage(error, `删除项目 ${project.appid} 失败，请稍后重试。`),
      );
    } finally {
      setDeletingAppid(null);
    }
  };

  const handleAssignRuleSet = async (ruleSetId: number) => {
    setUpdatingRuleSetId(ruleSetId);
    try {
      await assignRuleSetMutation.mutateAsync({
        rule_set_id: ruleSetId,
        enabled: true,
      });
      message.success("规则集绑定成功");
    } catch (error) {
      message.error(getRuleErrorMessage(error, "绑定规则集失败，请稍后重试。"));
    } finally {
      setUpdatingRuleSetId(null);
    }
  };

  const handleUnassignRuleSet = async (ruleSetId: number) => {
    setUpdatingRuleSetId(ruleSetId);
    try {
      await deleteAssignmentMutation.mutateAsync(ruleSetId);
      message.success("规则集解绑成功");
    } catch (error) {
      message.error(getRuleErrorMessage(error, "解绑规则集失败，请稍后重试。"));
    } finally {
      setUpdatingRuleSetId(null);
    }
  };

  const isSubmitting =
    createProjectMutation.isPending || updateProjectMutation.isPending;
  const isDeletePending = deletingAppid !== null;
  const formError =
    modalMode === "create"
      ? createProjectMutation.error
      : updateProjectMutation.error;

  return (
    <Space direction="vertical" size={16} style={{ display: "flex" }}>
      <Space
        align="start"
        style={{ justifyContent: "space-between", width: "100%" }}
      >
        <div>
          <Typography.Title level={3} style={{ margin: 0 }}>
            项目管理
          </Typography.Title>
          <Typography.Paragraph type="secondary" style={{ margin: "8px 0 0" }}>
            管理项目的创建、编辑、启停状态和删除操作。
          </Typography.Paragraph>
        </div>
        <Space>
          <Button
            disabled={isDeletePending}
            onClick={() => {
              void projectsQuery.refetch();
              void ruleSetsQuery.refetch();
            }}
            loading={projectsQuery.isFetching || ruleSetsQuery.isFetching}
          >
            刷新
          </Button>
          <Button
            type="primary"
            disabled={isDeletePending}
            onClick={handleCreateClick}
          >
            新建项目
          </Button>
        </Space>
      </Space>

      {projectsQuery.isLoading ? (
        <div style={{ display: "grid", minHeight: 240, placeItems: "center" }}>
          <Space direction="vertical" align="center" size={12}>
            <Spin size="large" />
            <Typography.Text type="secondary">正在加载项目列表...</Typography.Text>
          </Space>
        </div>
      ) : null}

      {showInitialError ? (
        <Result
          status="error"
          title="项目列表加载失败"
          subTitle={getErrorMessage(projectsQuery.error)}
          extra={
            <Button type="primary" onClick={() => void projectsQuery.refetch()}>
              重试
            </Button>
          }
        />
      ) : null}

      {!projectsQuery.isLoading && !showInitialError ? (
        <Space direction="vertical" size={16} style={{ display: "flex" }}>
          <Alert
            type="info"
            showIcon
            message={`共 ${projects.length} 个项目`}
          />
          {isDeletePending ? (
            <Alert
              type="info"
              showIcon
              message={`正在删除项目 ${deletingAppid}`}
              description="删除完成前，已临时禁用其他编辑和删除操作。"
            />
          ) : null}
          {isFormOpen && formError ? (
            <Alert
              type="error"
              showIcon
              message={modalMode === "create" ? "创建项目失败" : "保存项目失败"}
              description={getErrorMessage(formError)}
            />
          ) : null}
          {showRefreshError ? (
            <Alert
              type="warning"
              showIcon
              message="刷新失败，当前展示的是上次成功加载的数据"
              description={getErrorMessage(projectsQuery.error)}
            />
          ) : null}
          <ProjectsTable
            projects={projects}
            deletingAppid={deletingAppid}
            actionsDisabled={isDeletePending}
            onEdit={handleEditClick}
            onDelete={handleDelete}
          />
        </Space>
      ) : null}

      <ProjectFormModal
        open={isFormOpen}
        mode={modalMode}
        project={editingProject}
        confirmLoading={isSubmitting}
        ruleSetsSection={
          <ProjectRuleSetsPanel
            ruleSets={ruleSets}
            projectName={editingProject?.name}
            appid={editingAppid}
            assignments={assignmentsQuery.data ?? []}
            loading={assignmentsQuery.isFetching || ruleSetsQuery.isFetching}
            updatingRuleSetId={updatingRuleSetId}
            onAssign={handleAssignRuleSet}
            onUnassign={handleUnassignRuleSet}
          />
        }
        onCancel={handleCloseModal}
        onSubmit={handleSubmit}
      />
    </Space>
  );
}
